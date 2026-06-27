//! Item drag handler.
//!
//! This module handles the logic for dragging items across multiple inventory slots.
//! When a player clicks and drags with an item, they can distribute it across
//! multiple slots.
//!
//! Drag types:
//! - Left click drag - Evenly distributes items across slots
//! - Right click drag - Places one item in each slot
//! - Middle click drag (creative) - Creates full stacks in each slot (creative only)

use crate::screen_handler::{InventoryPlayer, ScreenHandler};
use pumpkin_data::item_stack::ItemStack;
use pumpkin_protocol::java::server::play::SlotActionType;
use tracing::warn;

pub async fn handle_quick_craft<S: ScreenHandler + ?Sized>(
    screen_handler: &mut S,
    slot_index: i32,
    button: i32,
    player: &dyn InventoryPlayer,
) {
    let drag_type = button & 3;
    let drag_button = (button >> 2) & 3;
    let behaviour = screen_handler.get_behaviour_mut();

    if drag_type == 0 {
        behaviour.drag_slots.clear();
    } else if drag_type == 1 {
        if slot_index < 0 {
            warn!("Invalid slot index for drag action: {slot_index}. Must be >= 0");
            return;
        }
        let cursor_stack = behaviour.cursor_stack.lock().await;

        let slot = &behaviour.slots[slot_index as usize];
        let stack_lock = slot.get_stack().await;
        let stack = stack_lock.lock().await;
        if !cursor_stack.is_empty()
            && slot.can_insert(&cursor_stack).await
            && (stack.are_items_and_components_equal(&cursor_stack) || stack.is_empty())
            && slot.get_max_item_count_for_stack(&stack).await > stack.item_count
        {
            behaviour.drag_slots.push(slot_index as u32);
        }
    } else if drag_type == 2 && !behaviour.drag_slots.is_empty() {
        // process drag end
        if behaviour.drag_slots.len() == 1 {
            let slot = behaviour.drag_slots[0] as i32;
            behaviour.drag_slots.clear();
            screen_handler
                .internal_on_slot_click(slot, drag_button, SlotActionType::Pickup, player)
                .await;

            return;
        }
        if drag_button == 2 && !player.has_infinite_materials() {
            return; // Only creative
        }

        let mut cursor_stack = behaviour.cursor_stack.lock().await;
        let initial_count = cursor_stack.item_count;
        for slot_index in &behaviour.drag_slots {
            let slot = behaviour.slots[*slot_index as usize].clone();
            let stack_lock = slot.get_stack().await;
            let stack = stack_lock.lock().await;

            if (stack.are_items_and_components_equal(&cursor_stack) || stack.is_empty())
                && slot.can_insert(&cursor_stack).await
            {
                let mut inserting_count = if drag_button == 0 {
                    initial_count / behaviour.drag_slots.len() as u8
                } else if drag_button == 1 {
                    1
                } else if drag_button == 2 {
                    cursor_stack.item_count = cursor_stack.get_max_stack_size();
                    cursor_stack.item_count
                } else {
                    warn!("Invalid drag button: {drag_button}");
                    return;
                };
                inserting_count = inserting_count
                    .min(
                        slot.get_max_item_count_for_stack(&stack)
                            .await
                            .saturating_sub(stack.item_count),
                    )
                    .min(cursor_stack.item_count);
                if inserting_count > 0 {
                    let mut stack_clone = stack.clone();
                    drop(stack);
                    if stack_clone.is_empty() {
                        stack_clone = cursor_stack.copy_with_count(0);
                    }
                    stack_clone.increment(inserting_count);
                    slot.set_stack(stack_clone).await;
                    if drag_button != 2 {
                        cursor_stack.decrement(inserting_count);
                    }
                    if cursor_stack.is_empty() {
                        *cursor_stack = ItemStack::EMPTY.clone();
                        break;
                    }
                }
            }
        }

        if drag_button == 2 {
            *cursor_stack = ItemStack::EMPTY.clone();
        }
        behaviour.drag_slots.clear();
    }
}
