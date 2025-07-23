use clap::Parser;
use deeplook_orderbook::historic_orderbook::get_historic_orderbook;
use deeplook_schema::schema::orderbook_snapshots;
use diesel::{Connection, PgConnection, RunQueryDsl};
use url::Url;

#[derive(Parser)]
#[clap(rename_all = "kebab-case", author, version)]
struct Args {
    #[clap(
        env,
        long,
        default_value = "postgres://postgres:postgrespw@localhost:5432/deeplook"
    )]
    database_url: Url,
    #[clap(env, long)]
    end_checkpoint: i64,
}

fn store_snapshot(pool_id: &str, end_checkpoint: i64, database_url: Url) {
    let new_snapshot_result = get_historic_orderbook(database_url.clone(), pool_id, end_checkpoint);

    let new_snapshot = match new_snapshot_result {
        Ok(v) => v,
        Err(e) => {
            println!("{:?}", e);
            return;
        }
    };

    let mut conn = PgConnection::establish(&database_url.as_str()).expect("Error connecting to DB");
    diesel::insert_into(orderbook_snapshots::table)
        .values(&new_snapshot)
        .execute(&mut conn)
        .expect("Failed storing snapshot");
    println!(
        "stored snapshot {}, {}",
        new_snapshot.checkpoint, new_snapshot.pool_id
    );
}

// export DATABASE_URL=...
// export END_CHECKPOINT=168980000
// cargo run -p deeplook-orderbook --bin update-snapshots
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let Args {
        database_url,
        end_checkpoint,
    } = Args::parse();

    let pool_ids = vec![
        "0xb663828d6217467c8a1838a03793da896cbe745b150ebd57d82f814ca579fc22",
        "0xf948981b806057580f91622417534f491da5f61aeaf33d0ed8e69fd5691c95ce",
        "0xe05dafb5133bcffb8d59f4e12465dc0e9faeaa05e3e342a08fe135800e3e4407",
        "0x1109352b9112717bd2a7c3eb9a416fff1ba6951760f5bdd5424cf5e4e5b3e65c",
        "0xa0b9ebefb38c963fd115f52d71fa64501b79d1adcb5270563f92ce0442376545",
        "0x4e2ca3988246e1d50b9bf209abb9c1cbfec65bd95afdacc620a36c67bdb8452f",
        "0x27c4fdb3b846aa3ae4a65ef5127a309aa3c1f466671471a806d8912a18b253e8",
        "0x0c0fdd4008740d81a8a7d4281322aee71a1b62c449eb5b142656753d89ebc060",
        "0xe8e56f377ab5a261449b92ac42c8ddaacd5671e9fec2179d7933dd1a91200eec",
        "0x183df694ebc852a5f90a959f0f563b82ac9691e42357e9a9fe961d71a1b809c8",
        "0x5661fc7f88fbeb8cb881150a810758cf13700bb4e1f31274a244581b37c303c3",
        "0x1fe7b99c28ded39774f37327b509d58e2be7fff94899c06d22b407496a6fa990",
        "0x56a1c985c1f1123181d6b881714793689321ba24301b3585eec427436eb1c76d",
        "0x81f5339934c83ea19dd6bcc75c52e83509629a5f71d3257428c2ce47cc94d08b",
        "0x20b9a3ec7a02d4f344aa1ebc5774b7b0ccafa9a5d76230662fdc0300bb215307",
        "0x126865a0197d6ab44bfd15fd052da6db92fd2eb831ff9663451bbfa1219e2af2",
    ];

    for pool_id in pool_ids.clone() {
        store_snapshot(pool_id, end_checkpoint, database_url.clone());
    }

    Ok(())
}
