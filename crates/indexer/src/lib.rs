use crate::handlers::convert_struct_tag;
use move_core_types::language_storage::StructTag;
use move_types::MoveStruct;
use url::Url;

pub mod handlers;
pub mod models;
pub mod utils;

pub const MAINNET_REMOTE_STORE_URL: &str = "https://checkpoints.mainnet.sui.io";
pub const TESTNET_REMOTE_STORE_URL: &str = "https://checkpoints.testnet.sui.io";

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum DeeplookEnv {
    Mainnet,
    Testnet,
}

/// Generates a function that returns the `StructTag` for a given event type,
/// switching between Mainnet and Testnet packages based on the `DeeplookEnv`.
///
/// # Arguments
///
/// * `fn_name` - The name of the function to generate.
/// * `path` - The path to the event type, relative to `models::deepbook` or `models::deepbook_testnet`.
///
/// # Example
///
/// ```rust
/// //impl DeeplookEnv {
/// //    event_type_fn!(balance_event_type, balance_manager::BalanceEvent);
/// //}
///
/// // Expands to:
/// //
/// // fn balance_event_type(&self) -> StructTag {
/// //     match self {
/// //         DeeplookEnv::Mainnet => {
/// //             use models::deepbook::balance_manager::BalanceEvent as Event;
/// //             convert_struct_tag(Event::struct_type())
/// //         },
/// //         DeeplookEnv::Testnet => {
/// //             use models::deepbook_testnet::balance_manager::BalanceEvent as Event;
/// //             convert_struct_tag(Event::struct_type())
/// //         }
/// //     }
/// // }
/// ```
///
macro_rules! event_type_fn {
    (
        $(#[$meta:meta])*
        $vis:vis $fn_name:ident,
        $($path:ident)::+
    ) => {
        $(#[$meta])*
        $vis fn $fn_name(&self) -> StructTag {
            match self {
                DeeplookEnv::Mainnet => {
                    use models::deepbook::$($path)::+ as Event;
                    convert_struct_tag(Event::struct_type())
                },
                DeeplookEnv::Testnet => {
                    use models::deepbook_testnet::$($path)::+ as Event;
                    convert_struct_tag(Event::struct_type())
                }
            }
        }
    };
}

impl DeeplookEnv {
    pub fn remote_store_url(&self) -> Url {
        let remote_store_url = match self {
            DeeplookEnv::Mainnet => MAINNET_REMOTE_STORE_URL,
            DeeplookEnv::Testnet => TESTNET_REMOTE_STORE_URL,
        };
        // Safe to unwrap on verified static URLs
        Url::parse(remote_store_url).unwrap()
    }

    event_type_fn!(pub balance_event_type, balance_manager::BalanceEvent);
    event_type_fn!(pub flash_loan_borrowed_event_type, vault::FlashLoanBorrowed);
    event_type_fn!(pub order_filled_event_type, order_info::OrderFilled);
    event_type_fn!(pub order_placed_event_type, order_info::OrderPlaced);
    event_type_fn!(pub order_modified_event_type, order::OrderModified);
    event_type_fn!(pub order_canceled_event_type, order::OrderCanceled);
    event_type_fn!(pub order_expired_event_type, order_info::OrderExpired);
    event_type_fn!(pub vote_event_type, state::VoteEvent);
    event_type_fn!(pub trade_params_update_event_type, governance::TradeParamsUpdateEvent);
    event_type_fn!(pub stake_event_type, state::StakeEvent);
    event_type_fn!(pub rebate_event_type, state::RebateEvent);
    event_type_fn!(pub proposal_event_type, state::ProposalEvent);
    event_type_fn!(pub price_added_event_type, deep_price::PriceAdded);
}
