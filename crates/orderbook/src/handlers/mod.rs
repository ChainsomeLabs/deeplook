use move_core_types::account_address::AccountAddress;
use sui_types::full_checkpoint_content::CheckpointTransaction;
use sui_types::transaction::{Command, TransactionDataAPI};

pub mod orderbook_order_fill_handler;
pub mod orderbook_order_update_handler;

const DEEPBOOK_PKG_ADDRESS: AccountAddress =
    AccountAddress::new(*deeplook_indexer::models::deepbook::registry::PACKAGE_ID.inner());

pub(crate) fn is_deepbook_tx(tx: &CheckpointTransaction) -> bool {
    tx.input_objects.iter().any(|obj| {
        obj.data
            .type_()
            .map(|t| t.address() == DEEPBOOK_PKG_ADDRESS)
            .unwrap_or_default()
    })
}

pub(crate) fn try_extract_move_call_package(tx: &CheckpointTransaction) -> Option<String> {
    let txn_kind = tx.transaction.transaction_data().kind();
    let first_command = txn_kind.iter_commands().next()?;
    if let Command::MoveCall(move_call) = first_command {
        Some(move_call.package.to_string())
    } else {
        None
    }
}
