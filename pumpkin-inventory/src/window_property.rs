//! Window property definitions.
//!
//! This module defines container-specific UI properties that need to be synchronized
//! between server and client. These include progress bars, fuel indicators, and
//! other visual elements in container screens.
//!
//! # Window Properties
//!
//! Properties are identified by a unique ID and sent to the client to update
//! the container's visual state:
//! - Furnace: Fire icon animation, smelting progress
//! - Enchantment table: Level requirements, available enchantments
//! - Brewing stand: Brew time, fuel level
//! - Anvil: Repair cost
//!
//! See the Minecraft wiki for property ID mappings.

/// Trait for types that can be converted to window property IDs.
pub trait WindowPropertyTrait {
    /// Converts this property to its protocol ID.
    fn to_id(self) -> i16;
}

/// A window property with a specific value.
///
/// Used to send property updates to the client (e.g., furnace progress bar).
pub struct WindowProperty<T: WindowPropertyTrait> {
    /// The property type being tracked (e.g., furnace fire icon, progress arrow).
    window_property: T,
    /// The current value of the property.
    value: i16,
}

impl<T: WindowPropertyTrait> WindowProperty<T> {
    /// Creates a new window property.
    ///
    /// # Arguments
    /// - `window_property` - The property type
    /// - `value` - The property value
    #[must_use]
    pub const fn new(window_property: T, value: i16) -> Self {
        Self {
            window_property,
            value,
        }
    }

    /// Converts this property to a tuple of (id, value).
    #[must_use]
    pub fn into_tuple(self) -> (i16, i16) {
        (self.window_property.to_id(), self.value)
    }
}

/// Furnace window properties.
pub enum Furnace {
    /// Fire icon animation level (0-250).
    FireIcon,
    /// Maximum fuel burn time.
    MaximumFuelBurnTime,
    /// Arrow progress animation (0-250).
    ProgressArrow,
    /// Maximum smelting progress time.
    MaximumProgress,
}

/// Enchantment table window properties.
pub enum EnchantmentTable {
    /// Experience level requirement for a specific slot.
    LevelRequirement { slot: u8 },
    /// Random seed for enchantment generation.
    EnchantmentSeed,
    /// Enchantment ID for a specific slot.
    EnchantmentId { slot: u8 },
    /// Enchantment level for a specific slot.
    EnchantmentLevel { slot: u8 },
}

impl EnchantmentTable {
    pub const LEVEL_REQUIREMENT_0: i16 = 0;
    pub const LEVEL_REQUIREMENT_1: i16 = 1;
    pub const LEVEL_REQUIREMENT_2: i16 = 2;
    pub const ENCHANTMENT_SEED: i16 = 3;
    pub const ENCHANTMENT_ID_0: i16 = 4;
    pub const ENCHANTMENT_ID_1: i16 = 5;
    pub const ENCHANTMENT_ID_2: i16 = 6;
    pub const ENCHANTMENT_LEVEL_0: i16 = 7;
    pub const ENCHANTMENT_LEVEL_1: i16 = 8;
    pub const ENCHANTMENT_LEVEL_2: i16 = 9;
}

impl WindowPropertyTrait for EnchantmentTable {
    fn to_id(self) -> i16 {
        use EnchantmentTable::{
            EnchantmentId, EnchantmentLevel, EnchantmentSeed, LevelRequirement,
        };

        match self {
            LevelRequirement { slot } => match slot {
                0 => Self::LEVEL_REQUIREMENT_0,
                1 => Self::LEVEL_REQUIREMENT_1,
                2 => Self::LEVEL_REQUIREMENT_2,
                _ => slot as i16,
            },
            EnchantmentSeed => Self::ENCHANTMENT_SEED,
            EnchantmentId { slot } => match slot {
                0 => Self::ENCHANTMENT_ID_0,
                1 => Self::ENCHANTMENT_ID_1,
                2 => Self::ENCHANTMENT_ID_2,
                _ => 4 + slot as i16,
            },
            EnchantmentLevel { slot } => match slot {
                0 => Self::ENCHANTMENT_LEVEL_0,
                1 => Self::ENCHANTMENT_LEVEL_1,
                2 => Self::ENCHANTMENT_LEVEL_2,
                _ => 7 + slot as i16,
            },
        }
    }
}

/// Beacon window properties.
pub enum Beacon {
    /// Effect power level (1-4).
    PowerLevel,
    /// First selected potion effect ID.
    FirstPotionEffect,
    /// Second selected potion effect ID.
    SecondPotionEffect,
}

/// Anvil window properties.
pub enum Anvil {
    /// Total repair cost in experience levels.
    RepairCost,
}

impl WindowPropertyTrait for Anvil {
    fn to_id(self) -> i16 {
        match self {
            Self::RepairCost => 0,
        }
    }
}

/// Brewing stand window properties.
pub enum BrewingStand {
    /// Brewing progress (0-400).
    BrewTime,
    /// Fuel time remaining (0-20).
    FuelTime,
}

/// Stonecutter window properties.
pub enum Stonecutter {
    /// ID of the selected recipe.
    SelectedRecipe,
}

/// Loom window properties.
pub enum Loom {
    /// ID of the selected pattern.
    SelectedPattern,
}

/// Lectern window properties.
pub enum Lectern {
    /// Current page number being viewed.
    PageNumber,
}
