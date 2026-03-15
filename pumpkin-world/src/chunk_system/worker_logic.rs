use super::chunk_state::{Chunk, StagedChunkEnum};
use super::generation_cache::Cache;
use super::{ChunkPos, IOLock};
use crate::ProtoChunk;
use crate::chunk::format::LightContainer;
use crate::chunk::io::LoadedData::Loaded;
use crate::level::Level;
use crossfire::compat::AsyncRx;
use pumpkin_config::lighting::LightingEngineConfig;
use pumpkin_data::chunk::ChunkStatus;
use pumpkin_data::chunk_gen_settings::GenerationSettings;
use rayon::prelude::*;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use tracing::{debug, error};

pub enum RecvChunk {
    IO(Chunk),
    Generation(Cache),
    GenerationFailure {
        pos: ChunkPos,
        stage: StagedChunkEnum,
        error: String,
    },
}

/// Checks if a chunk needs relighting based on the current lighting configuration.
/// Returns true if the chunk has uniform lighting but the server is in default mode.
fn needs_relighting(chunk: &crate::chunk::ChunkData, config: &LightingEngineConfig) -> bool {
    if *config != LightingEngineConfig::Default {
        return false;
    }

    if chunk.light_populated.load(Relaxed) {
        return false;
    }

    let engine = chunk.light_engine.lock().expect("Mutex poisoned");

    // SMARTER HEURISTIC: Check if light has actually propagated horizontally (values 1-14).
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
    let dimension = level.world_gen.dimension;
    let (t_send, mut t_recv) = tokio::sync::mpsc::channel(1);

    while let Ok(pos) = recv.recv().await {
        // Instead of yield_now(), use a 1ms sleep to force a real gap for the network task
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;

        // 2. Thread-safe Lock Check: Check, then DROP the guard before sleeping
        let is_locked = {
            let data = lock.0.lock().unwrap();
            data.contains_key(&pos)
        };

        if is_locked {
            // If locked, we MUST back off significantly in Release mode
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            // Send ourselves back to the end of the queue
            continue;
        }

        level
            .chunk_saver
            .fetch_chunks(&level.level_folder, &[pos], t_send.clone())
            .await;

        match t_recv.recv().await {
            Some(Loaded(chunk)) => {
                let level_clone = level.clone();
                let send_clone = send.clone();

                rayon::spawn(move || {
                    let processed = if chunk.status == ChunkStatus::Full {
                        if needs_relighting(&chunk, &level_clone.lighting_config) {
                            // Neighborhood Gate: Only relight if neighbors are in DashMap
                            let mut ready = true;
                            for nx in -1..=1 {
                                for nz in -1..=1 {
                                    if nx == 0 && nz == 0 { continue; }
                                    let neighbor = ChunkPos::new(pos.x + nx, pos.y + nz);
                                    if !level_clone.loaded_chunks.contains_key(&neighbor) {
                                        ready = false; break;
                                    }
                                }
                                if !ready { break; }
                            }

                            if ready {
                                let mut proto = ProtoChunk::from_chunk_data(&chunk, &level_clone.world_gen.dimension, level_clone.world_gen.default_block, biome_mixer_seed);
                                let sc = proto.light.sky_light.len();
                                proto.light.sky_light = (0..sc).map(|_| LightContainer::new_empty(0)).collect();
                                proto.light.block_light = (0..sc).map(|_| LightContainer::new_empty(0)).collect();
                                proto.stage = StagedChunkEnum::Features;
                                RecvChunk::IO(Chunk::Proto(Box::new(proto)))
                            } else {
                                RecvChunk::IO(Chunk::Level(chunk))
                            }
                        } else {
                            RecvChunk::IO(Chunk::Level(chunk))
                        }
                    } else {
                        RecvChunk::IO(Chunk::Proto(Box::new(ProtoChunk::from_chunk_data(&chunk, &level_clone.world_gen.dimension, level_clone.world_gen.default_block, biome_mixer_seed))))
                    };
                    let _ = send_clone.send((pos, processed));
                });
            }
            _ => {
                // REQUIRED: Trigger generation for missing chunks
                let fallback = RecvChunk::IO(Chunk::Proto(Box::new(ProtoChunk::new(
                    pos.x, pos.y, &dimension, level.world_gen.default_block, biome_mixer_seed,
                ))));
                if send.send((pos, fallback)).is_err() { break; }
            }
        }
    }
    debug!("io read thread stop");
}

pub async fn io_write_work(recv: AsyncRx<Vec<(ChunkPos, Chunk)>>, level: Arc<Level>, lock: IOLock) {
    while let Ok(data) = recv.recv().await {
        let level_clone = level.clone();
        let lock_clone = lock.clone();

        // DECOUPLED BACKGROUND WRITE: Prevents save-batches from stalling the channel
        tokio::task::spawn(async move {
            let (rayon_tx, rayon_rx) = tokio::sync::oneshot::channel();
            let rayon_level = level_clone.clone(); 
            
            rayon::spawn(move || {
                let processed: Vec<_> = data.into_par_iter().map(|(pos, chunk)| {
                    match chunk {
                        Chunk::Level(c) => (pos, c),
                        Chunk::Proto(c) => {
                            let mut temp = Chunk::Proto(c);
                            temp.upgrade_to_level_chunk(&rayon_level.world_gen.dimension, &rayon_level.lighting_config);
                            let Chunk::Level(upgraded) = temp else { panic!("Upgrade failed") };
                            (pos, upgraded)
                        }
                    }
                }).collect();
                let _ = rayon_tx.send(processed);
            });

            if let Ok(vec) = rayon_rx.await {
                let pos_list: Vec<_> = vec.iter().map(|(p, _)| *p).collect();
                let _ = level_clone.chunk_saver.save_chunks(&level_clone.level_folder, vec).await;
                
                let mut guard = lock_clone.0.lock().unwrap();
                let mut needs_notify = false;
                for p in pos_list {
                    if let Entry::Occupied(mut entry) = guard.entry(p) {
                        let rc = entry.get_mut();
                        if *rc == 1 {
                            entry.remove();
                            needs_notify = true;
                        } else {
                            *rc -= 1;
                        }
                    }
                }
                if needs_notify {
                    lock_clone.1.notify_all();
                }
            }
        });
    }
}

pub fn generation_work(
    recv: crossfire::compat::MRx<(ChunkPos, Cache, StagedChunkEnum)>,
    send: crossfire::compat::MTx<(ChunkPos, RecvChunk)>,
    level: Arc<Level>,
) {
    let settings = GenerationSettings::from_dimension(&level.world_gen.dimension);
    // Persistent loop: never exit unless channel is closed
    loop {
        let (pos, mut cache, stage) = match recv.recv() {
            Ok(data) => data,
            Err(_) => break, // Channel closed
        };

        let level_clone = level.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cache.advance(
                stage,
                &level_clone.lighting_config,
                level_clone.block_registry.as_ref(),
                settings,
                &level_clone.world_gen.random_config,
                &level_clone.world_gen.terrain_cache,
                &level_clone.world_gen.base_router,
                level_clone.world_gen.dimension,
            );
            cache
        }));

        match result {
            Ok(cache) => {
                let _ = send.send((pos, RecvChunk::Generation(cache)));
            }
            Err(_) => {
                error!("Chunk generation panicked at {pos:?}");
                let _ = send.send((
                    pos,
                    RecvChunk::GenerationFailure {
                        pos, stage, error: "Panic during generation".to_string(),
                    },
                ));
            }
        }
    }
}