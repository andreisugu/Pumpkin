use super::chunk_state::{Chunk, StagedChunkEnum};
use super::generation_cache::Cache;
use super::{ChunkPos, IOLock};
use crate::ProtoChunk;
use crate::chunk::format::LightContainer;
use crate::chunk::io::LoadedData;
use crate::chunk::io::LoadedData::Loaded;
use crate::level::Level;
use crossfire::compat::AsyncRx;
use itertools::Itertools;
use pumpkin_config::lighting::LightingEngineConfig;
use pumpkin_data::chunk::ChunkStatus;
use pumpkin_data::chunk_gen_settings::GenerationSettings;
use rayon::prelude::*;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use tracing::{debug, error, warn};

pub enum RecvChunk {
    IO(Chunk),
    Generation(Cache),
    GenerationFailure {
        pos: ChunkPos,
        stage: StagedChunkEnum,
        error: String,
    },
}

/// Checks if a chunk needs relighting based on the current lighting configuration
/// Returns true if the chunk has uniform lighting (from full/dark mode) but the server
/// is now running in default mode (which needs proper lighting calculation)
/// Checks if a chunk needs relighting based on the current lighting configuration
/// Returns true if the chunk has uniform lighting (from full/dark mode) but the server
/// is now running in default mode (which needs proper lighting calculation)
fn needs_relighting(chunk: &crate::chunk::ChunkData, config: &LightingEngineConfig) -> bool {
    if *config != LightingEngineConfig::Default {
        return false;
    }

    // If the chunk says it's already lit, believe it.
    if chunk.light_populated.load(Relaxed) {
        return false;
    }

    let engine = chunk.light_engine.lock().expect("Mutex poisoned");

    // SMARTER HEURISTIC: Check if light has actually propagated horizontally (values 1-14).
    // If a chunk only has 0 and 15, it's just straight-down sunbeams (broken horizontal light).
    let has_propagated_light = engine.sky_light.iter().any(|lc| match lc {
        LightContainer::Full(data) => data.iter().any(|&b| {
            let low = b & 0x0F;
            let high = (b >> 4) & 0x0F;
            (low > 0 && low < 15) || (high > 0 && high < 15)
        }),
        LightContainer::Empty(val) => *val > 0 && *val < 15,
    }) || engine.block_light.iter().any(|lc| match lc {
        LightContainer::Full(data) => data.iter().any(|&b| {
            let low = b & 0x0F;
            let high = (b >> 4) & 0x0F;
            (low > 0 && low < 15) || (high > 0 && high < 15)
        }),
        LightContainer::Empty(val) => *val > 0 && *val < 15,
    });

    // If it HAS NOT propagated properly, it needs a relight!
    !has_propagated_light
}

pub async fn io_read_work(
    recv: crossfire::compat::MAsyncRx<ChunkPos>,
    send: crossfire::compat::MTx<(ChunkPos, RecvChunk)>,
    level: Arc<Level>,
    lock: IOLock,
) {
    use crate::biome::hash_seed;
    debug!("io read thread start");
    let biome_mixer_seed = hash_seed(level.world_gen.random_config.seed);
    let dimension = &level.world_gen.dimension;
    let (t_send, mut t_recv) = tokio::sync::mpsc::channel(1);

    // Cleaner loop and async recv
    while let Ok(pos) = recv.recv().await {
        // --- ASYNC LOCK WAIT ---
        loop {
            let is_locked = {
                let data = lock.0.lock().unwrap();
                data.contains_key(&pos)
            };
            if !is_locked {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        level
            .chunk_saver
            .fetch_chunks(&level.level_folder, &[pos], t_send.clone())
            .await;

        let data = match t_recv.recv().await {
            Some(res) => res,
            None => break,
        };

        match data {
            Loaded(chunk) => {
                let level_clone = level.clone();
                let is_full = chunk.status == ChunkStatus::Full;
                let (rayon_tx, rayon_rx) = tokio::sync::oneshot::channel();

                // CPU WORK -> RAYON
                rayon::spawn(move || {
                    let processed = if is_full {
                        if needs_relighting(&chunk, &level_clone.lighting_config) {
                            let mut proto = ProtoChunk::from_chunk_data(
                                &chunk,
                                &level_clone.world_gen.dimension,
                                level_clone.world_gen.default_block,
                                biome_mixer_seed,
                            );
                            let sc = proto.light.sky_light.len();
                            proto.light.sky_light =
                                (0..sc).map(|_| LightContainer::new_empty(0)).collect();
                            proto.light.block_light =
                                (0..sc).map(|_| LightContainer::new_empty(0)).collect();
                            proto.stage = StagedChunkEnum::Features;
                            RecvChunk::IO(Chunk::Proto(Box::new(proto)))
                        } else {
                            RecvChunk::IO(Chunk::Level(chunk))
                        }
                    } else {
                        RecvChunk::IO(Chunk::Proto(Box::new(ProtoChunk::from_chunk_data(
                            &chunk,
                            &level_clone.world_gen.dimension,
                            level_clone.world_gen.default_block,
                            biome_mixer_seed,
                        ))))
                    };
                    let _ = rayon_tx.send(processed);
                });

                let processed_chunk = rayon_rx.await.expect("Rayon worker died");

                if let RecvChunk::IO(Chunk::Proto(_)) = &processed_chunk {
                    if is_full {
                        debug!("Chunk {pos:?} relighting triggered");
                    }
                }

                if send.send((pos, processed_chunk)).is_err() {
                    break;
                }
                continue;
            }
            LoadedData::Missing(_) => {}
            LoadedData::Error(_) => {
                warn!("chunk data read error pos: {pos:?}. regenerating");
            }
        }
        // Final send for missing/error cases (regenerate)
        if send
            .send((
                pos,
                RecvChunk::IO(Chunk::Proto(Box::new(ProtoChunk::new(
                    pos.x,
                    pos.y,
                    dimension,
                    level.world_gen.default_block,
                    biome_mixer_seed,
                )))),
            ))
            .is_err()
        {
            break;
        }
    }
    debug!("io read thread stop");
}

pub async fn io_write_work(recv: AsyncRx<Vec<(ChunkPos, Chunk)>>, level: Arc<Level>, lock: IOLock) {
    loop {
        let data = match recv.recv().await {
            Ok(d) => d,
            Err(_) => break,
        };

        let level_clone = level.clone();
        let (rayon_tx, rayon_rx) = tokio::sync::oneshot::channel();

        // PARALLEL COMPUTE -> RAYON
        rayon::spawn(move || {
            let processed_vec: Vec<_> = data
                .into_par_iter()
                .map(|(pos, chunk)| match chunk {
                    Chunk::Level(chunk) => (pos, chunk),
                    Chunk::Proto(chunk) => {
                        let mut temp = Chunk::Proto(chunk);
                        temp.upgrade_to_level_chunk(
                            &level_clone.world_gen.dimension,
                            &level_clone.lighting_config,
                        );
                        let Chunk::Level(chunk) = temp else {
                            panic!("Upgrade failed")
                        };
                        (pos, chunk)
                    }
                })
                .collect();
            let _ = rayon_tx.send(processed_vec);
        });

        let vec = rayon_rx.await.expect("Rayon write-prep failed");
        let pos = vec.iter().map(|(pos, _)| *pos).collect_vec();

        if let Err(e) = level
            .chunk_saver
            .save_chunks(&level.level_folder, vec)
            .await
        {
            error!("Save error: {:?}", e);
        }

        // BATCHED LOCK RELEASE (Thread-Safe)
        let mut needs_notify = false;
        {
            let mut data = lock.0.lock().unwrap();
            for i in pos {
                if let Entry::Occupied(mut entry) = data.entry(i) {
                    let rc = entry.get_mut();
                    if *rc == 1 {
                        entry.remove();
                        needs_notify = true;
                    } else {
                        *rc -= 1;
                    }
                }
            }
        }
        if needs_notify {
            lock.1.notify_all();
        }
    }
}

pub fn generation_work(
    recv: crossfire::compat::MRx<(ChunkPos, Cache, StagedChunkEnum)>,
    send: crossfire::compat::MTx<(ChunkPos, RecvChunk)>,
    level: Arc<Level>,
) {
    let settings = GenerationSettings::from_dimension(&level.world_gen.dimension);

    loop {
        let (pos, mut cache, stage) = if let Ok(data) = recv.recv() {
            data
        } else {
            debug!("generation channel closed, exiting");
            break;
        };

        // Run generation with panic catching
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cache.advance(
                stage,
                &level.lighting_config,
                level.block_registry.as_ref(),
                settings,
                &level.world_gen.random_config,
                &level.world_gen.terrain_cache,
                &level.world_gen.base_router,
                level.world_gen.dimension,
            );
            cache // Return cache on success
        }));

        match result {
            Ok(cache) => {
                if send.send((pos, RecvChunk::Generation(cache))).is_err() {
                    break;
                }
            }
            Err(payload) => {
                let msg = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| {
                        payload
                            .downcast_ref::<String>()
                            .map(std::string::String::as_str)
                    })
                    .unwrap_or("Unknown panic payload");

                error!("Chunk generation FAILED at {pos:?} ({stage:?}): {msg}");

                // Send failure notification
                let _ = send.send((
                    pos,
                    RecvChunk::GenerationFailure {
                        pos,
                        stage,
                        error: msg.to_string(),
                    },
                ));
            }
        }
    }
}
