//! Recipe-related types and traits.
//!
//! This module defines the interfaces for recipe handling in crafting systems.
//! It provides traits for screen handlers that can find recipes and inventories
//! that can serve as recipe input.
//!
//! # Recipe System
//!
//! The recipe system involves:
//! - [`RecipeFinderScreenHandler`] - Screen handlers that can find matching recipes
//! - [`RecipeInputInventory`] - Inventories that provide crafting input
//! - [`RecipeMatcher`] - Helper for matching items to recipes
//! - [`RecipeFinder`] - Helper for finding recipes

use std::future::Future;
use std::pin::Pin;

use pumpkin_data::item_stack::ItemStack;
use pumpkin_world::inventory::Inventory;

/// Type alias for async recipe operations.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Helper struct for matching recipe ingredients.
// RecipeMatcher.java
pub struct RecipeMatcher;

/// Helper struct for finding recipes.
// RecipeFinder.java
pub struct RecipeFinder;

/// Trait for screen handlers that can find crafting recipes.
///
/// Screen handlers implementing this trait can search for recipes
/// that match the current input inventory state.
// AbstractRecipeScreenHandle.java
pub trait RecipeFinderScreenHandler {}

/// Represents input ingredients passed to recipe matching logic.
#[derive(Clone)]
pub struct CraftingInput {
    pub width: usize,
    pub height: usize,
    pub items: Vec<ItemStack>,
}

impl CraftingInput {
    #[must_use]
    pub const fn new(width: usize, height: usize, items: Vec<ItemStack>) -> Self {
        Self {
            width,
            height,
            items,
        }
    }

    #[must_use]
    pub fn get_item(&self, index: usize) -> &ItemStack {
        &self.items[index]
    }

    #[must_use]
    pub fn get_item_xy(&self, x: usize, y: usize) -> &ItemStack {
        &self.items[x + y * self.width]
    }

    #[must_use]
    pub const fn size(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.iter().all(ItemStack::is_empty)
    }

    #[must_use]
    pub fn of(width: usize, height: usize, items: &[ItemStack]) -> Self {
        Self::of_positioned(width, height, items).input
    }

    #[must_use]
    pub fn of_positioned(width: usize, height: usize, items: &[ItemStack]) -> PositionedCraftingInput {
        if width == 0 || height == 0 {
            return PositionedCraftingInput {
                input: Self::new(0, 0, Vec::new()),
                left: 0,
                top: 0,
            };
        }

        let mut left = width - 1;
        let mut right = 0;
        let mut top = height - 1;
        let mut bottom = 0;
        let mut has_any = false;

        for y in 0..height {
            let mut row_empty = true;
            for x in 0..width {
                let item = &items[x + y * width];
                if !item.is_empty() {
                    left = left.min(x);
                    right = right.max(x);
                    row_empty = false;
                    has_any = true;
                }
            }
            if !row_empty {
                top = top.min(y);
                bottom = bottom.max(y);
            }
        }

        if !has_any {
            return PositionedCraftingInput {
                input: Self::new(0, 0, Vec::new()),
                left: 0,
                top: 0,
            };
        }

        let new_width = right - left + 1;
        let new_height = bottom - top + 1;

        if new_width == width && new_height == height {
            PositionedCraftingInput {
                input: Self::new(width, height, items.to_vec()),
                left,
                top,
            }
        } else {
            let mut new_items = Vec::with_capacity(new_width * new_height);
            for y in 0..new_height {
                for x in 0..new_width {
                    let index = (x + left) + (y + top) * width;
                    new_items.push(items[index].clone());
                }
            }
            PositionedCraftingInput {
                input: Self::new(new_width, new_height, new_items),
                left,
                top,
            }
        }
    }
}

/// Represents a crafting input aligned to its topmost-leftmost bounding box.
#[derive(Clone)]
pub struct PositionedCraftingInput {
    pub input: CraftingInput,
    pub left: usize,
    pub top: usize,
}

/// Trait for inventories that serve as recipe input.
///
/// Crafting grids implement this trait to provide their dimensions
/// and item access for recipe matching.
pub trait RecipeInputInventory: Inventory {
    /// Gets the width of the crafting grid.
    fn get_width(&self) -> usize;

    /// Gets the height of the crafting grid.
    fn get_height(&self) -> usize;

    /// Creates a flat `CraftingInput` of the inventory's current state.
    fn create_recipe_input(&self) -> BoxFuture<'_, CraftingInput>;

    /// Creates a positioned `PositionedCraftingInput` of the inventory's current state.
    fn create_positioned_recipe_input(&self) -> BoxFuture<'_, PositionedCraftingInput>;
}
