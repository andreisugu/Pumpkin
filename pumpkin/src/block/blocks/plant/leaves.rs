use pumpkin_data::block_properties::{BlockProperties, EnumVariants, Integer1To7};
use pumpkin_data::{Block, BlockDirection, tag};
use pumpkin_data::tag::Taggable;
use pumpkin_macros::pumpkin_block_from_tag;
use pumpkin_util::math::position::BlockPos;
use pumpkin_world::world::BlockFlags;
use pumpkin_world::BlockStateId;

use crate::block::{
    BlockBehaviour, BlockFuture, GetStateForNeighborUpdateArgs, OnPlaceArgs,
    OnScheduledTickArgs, PlayerPlacedArgs, RandomTickArgs,
};
use crate::world::World;

type LeavesProperties = pumpkin_data::block_properties::OakLeavesLikeProperties;

/// Shared implementation for every leaf-type block (oak, birch, spruce, …).
///
/// ## Vanilla behaviour summary
///
/// * **`distance`** (1–7): Updated on placement and neighbour updates.
///   - Logs have an implicit distance of 0.
///   - A leaf's distance = min(neighbour distances) + 1, clamped to 7.
/// * **`persistent`**: `true` when placed by a player.  Persistent leaves
///   never decay.
/// * **Decay trigger**: When a random tick fires and the leaf is not
///   persistent and `distance == 7`, the leaf decays (via `break_block`).
///
/// ## Distance propagation
///
/// When a log is broken, `get_state_for_neighbor_update` cascades distance
/// recalculations through the leaf tree.  Leaves that end up at distance 7
/// are disconnected from any log and will decay on their next random tick.
#[pumpkin_block_from_tag("minecraft:leaves")]
pub struct LeavesBlock;

impl LeavesBlock {
    /// Compute the `distance` value for a leaf at `pos` based on its
    /// six direct neighbours.
    async fn compute_distance(world: &World, pos: &BlockPos, _current: &Block) -> Integer1To7 {
        let mut min_neighbor_distance: u16 = 7;

        for direction in BlockDirection::all() {
            let neighbor_pos = pos.offset(direction.to_offset());
            let (neighbor_block, neighbor_state_id) =
                world.get_block_and_state_id(&neighbor_pos).await;

            if neighbor_block.has_tag(&tag::Block::MINECRAFT_LOGS) {
                // Logs have implicit distance 0 → this leaf becomes 1.
                return Integer1To7::L1;
            }

            if neighbor_block.has_tag(&tag::Block::MINECRAFT_LEAVES)
                && LeavesProperties::handles_block_id(neighbor_block.id)
            {
                let neighbor_props =
                    LeavesProperties::from_state_id(neighbor_state_id, neighbor_block);
                let d = neighbor_props.distance.to_index() + 1; // to_index(): L1→0 .. L7→6
                min_neighbor_distance = min_neighbor_distance.min(d);
            }
        }

        // min_neighbor_distance is 0-based index; +1 for our own distance.
        let own_index = (min_neighbor_distance + 1).min(6); // clamp to L7 index (6)
        Integer1To7::from_index(own_index)
    }

}

impl BlockBehaviour for LeavesBlock {
    // ── Placement ────────────────────────────────────────────────────

    fn on_place<'a>(&'a self, args: OnPlaceArgs<'a>) -> BlockFuture<'a, BlockStateId> {
        Box::pin(async move {
            let distance =
                Self::compute_distance(args.world, args.position, args.block).await;
            let mut props = LeavesProperties::default(args.block);
            props.distance = distance;
            // Waterlogging handled elsewhere by the engine.
            props.to_state_id(args.block)
        })
    }

    /// Mark player-placed leaves as persistent so they never decay.
    fn player_placed<'a>(&'a self, args: PlayerPlacedArgs<'a>) -> BlockFuture<'a, ()> {
        Box::pin(async move {
            let state_id = args.world.get_block_state_id(args.position).await;
            let mut props = LeavesProperties::from_state_id(state_id, args.block);
            if !props.persistent {
                props.persistent = true;
                args.world
                    .clone()
                    .set_block_state(
                        args.position,
                        props.to_state_id(args.block),
                        BlockFlags::NOTIFY_ALL,
                    )
                    .await;
            }
        })
    }

    // ── Neighbour propagation ────────────────────────────────────────

    fn get_state_for_neighbor_update<'a>(
        &'a self,
        args: GetStateForNeighborUpdateArgs<'a>,
    ) -> BlockFuture<'a, BlockStateId> {
        Box::pin(async move {
            let new_distance =
                Self::compute_distance(args.world, args.position, args.block).await;
            let mut props = LeavesProperties::from_state_id(args.state_id, args.block);
            if new_distance != props.distance {
                props.distance = new_distance;
                return props.to_state_id(args.block);
            }
            args.state_id
        })
    }

    // ── Random tick → leaf decay ────────────────────────────────────

    fn random_tick<'a>(&'a self, args: RandomTickArgs<'a>) -> BlockFuture<'a, ()> {
        Box::pin(async move {
            let state_id = args.world.get_block_state_id(args.position).await;
            let props = LeavesProperties::from_state_id(state_id, args.block);

            // Persistent leaves never decay; leaves at distance < 7 are
            // still connected to a log.
            if props.persistent || props.distance != Integer1To7::L7 {
                return;
            }

            // Leaf at distance 7 and not persistent → decay immediately.
            // `break_block` handles loot drops, waterlogging, particles,
            // and cascading neighbor updates.
            args.world
                .break_block(args.position, None, BlockFlags::NOTIFY_ALL)
                .await;
        })
    }

    // ── Scheduled tick (fallback / vanilla-compatible path) ──────────

    fn on_scheduled_tick<'a>(&'a self, args: OnScheduledTickArgs<'a>) -> BlockFuture<'a, ()> {
        Box::pin(async move {
            let state_id = args.world.get_block_state_id(args.position).await;
            let props = LeavesProperties::from_state_id(state_id, args.block);

            if props.persistent {
                return;
            }

            // Recompute distance — it may have been updated by neighbour
            // propagation since the tick was scheduled.
            let distance =
                Self::compute_distance(args.world, args.position, args.block).await;
            if distance != props.distance {
                let mut new_props = props;
                new_props.distance = distance;
                args.world
                    .clone()
                    .set_block_state(
                        args.position,
                        new_props.to_state_id(args.block),
                        BlockFlags::NOTIFY_ALL,
                    )
                    .await;
            }
        })
    }
}
