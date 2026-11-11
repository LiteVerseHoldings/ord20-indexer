## Local token-trade checkpoint candidate - not deployed

This candidate uses the organization repository at base commit
`dfc23efe0dd95188b410dacc7cd407f88a89cf25`. The running server executable is
confirmed to be the live Rust ord20 process, but carries no verifiable embedded
repository revision. Its exact build provenance remains unproven. Do not treat
this checkout as a certified byte-for-byte source match for that binary.

A public-upstream prototype was used for comparison; this review candidate is
based on the organization's source.
This port preserves the organization's first-output genesis allocation and
fee-recipient behavior. An isolated synthetic replay compares the original
organization ledger against this candidate: balances, token trackers and transfer
records agree; send history intentionally records the actual spending transaction,
height and time instead of the inscription creation identity. This is not a full
historical replay or proof of general consensus correctness.

Each block commits its ledger mutations, last_height and canonical checkpoint in
one PostgreSQL transaction. Reorgs, missing required previous-output metadata,
SQL failures, gaps and panics refuse advancement. Bootstrap only permits an empty
ledger at the reference activation boundary. A populated uncheckpointed ledger
is refused; do not seed its marker manually or run this beside the old writer.
Keep the production ledger and prior binary intact. Source verification and an
audited independent replay remain release gates.

The local integration command is `bash tests/run-isolated.sh` in a NEW isolated
Linux container with PostgreSQL 15, PHP/PDO pgsql and Rust, this source copied to
its work directory and the paired Litescribe checkout mounted read-only at
`/consumer`. It creates only synthetic DB schemas on a Unix socket; no real wallet
or production DB is involved. Test artifacts are retained.

New candidate binaries accept `--build-info` before loading configuration or
opening any database. The review build embeds its base revision and a SHA-256
fingerprint of the exact application source and Cargo lockfile. An unrecorded
build reports that explicitly. Never try this argument on the old production
binary: its command-line behavior has not been verified as read-only.

Actual wallet-signed SegWit and Taproot swaps passed Litecoin regtest acceptance,
Ord delivery and exact native LTC-20 seller credit checks using the paired
[Ord API candidate](https://github.com/LiteVerseHoldings/ord-litecoin/tree/api-15).
The optional test-only `native-chain-regtest` feature consumes that disposable
chain and its synthetic swap fixtures; it refuses mainnet and never produces a
mainnet-ready checkpoint. Full historical mainnet replay/reconciliation, live
build provenance and the hosted purchase flow with the packaged wallet remain
release gates. Nothing was deployed, restarted or deleted by this work.

<!-- PROJECT LOGO -->
<div align="center">
  <h3 align="center">Ord20 Indexer (ord-20) library</h3>

  <p align="center">
    This indexer fully implements the ordinals meta protocol of brc-20/ltc-20/drc-20 to hereby be known as ord-20.
    <br />
    <br />
    <a href="https://github.com/ynohtna92/ord20-indexer/issues">Report Bug</a>
    ·
    <a href="https://github.com/ynohtna92/ord20-indexer/issues">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>

<!-- ABOUT THE PROJECT -->
## About The Project

Developers of the meta protocol ord-20 can integrate this indexer in the code according to their needs.

Once the indexer is started and connected to a local ordinals server, it will process each block one by one and populate the postgreSQL database with a detailed list of all token information, holder balances, and inscription transfer history.

<!-- GETTING STARTED -->
## Getting Started
Ord20 Indexer and its prerequisites can be installed from the command line.

### Prerequisites
1. Install Rust and its Dependencies
    ```sh
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```
    This project is developed with rust and can be built from source.

    ```sh
   # Linux dependencies
    sudo apt install libpq-dev libssl-dev
    ```

2. Install PostgreSQL
* PostgreSQL (Linux)
  ```sh
  sudo apt-get install postgresql postgresql-client
  ```
  Start PostgreSQL CLI as the `postgres` user
  ```
  sudo -u postgres psql
  ```
  Modify the PostgreSQL password
  ```
  sudo -u postgres psql
  \password
  
  # Set a unique password and modify .env otherwise set to 'password'
  ```
* PostgreSQL (Mac)
  ```sh
  brew install postgresql
  ```
  Start the server:
  ```
  brew services start postgresql
  ```

3. Install Diesel
    ```sh
    cargo install diesel_cli --no-default-features --features postgres
    ```

### Installation

1. Clone the repo and build the binary
   ```sh
   git clone https://github.com/ynohtna92/ord20-indexer.git
   cd ord20-indexer
   cargo build --release
   ```

2. Download Litecoin core 0.21.2.2 from official website https://litecoin.org/
    ```sh
    tar -zxvf litecoin-0.21.2.2-x86_64-linux-gnu.tar.gz
    sudo install -m 0755 -o root -g root -t /usr/local/bin ~/litecoin-0.21.2.2/bin/*
    ```

3. Edit the `litecoin.conf` file of the full node
    ```
    cd ~/.litecoin
    vi litecoin.conf
    ```
    Add the following configurations and save
    ```
    txindex=1
    rpcserialversion=1
    ```

5. Run the Litecoin full node until it is fully synchronised (depend on your network, it will generally take up to several hours to days to sync)

6. Clone the ordinals repo and build the ord server.
    ```sh
    git clone https://github.com/ynohtna92/ord-litecoin.git
    cd ord-litecoin
    git checkout api-15
    cargo build --release
    ```

7. Run the ordinals server, and allow it to fully synchronise.
    ```
    ./ord server -j
    ```

8. Configure the ord20-indexer in your `.env` file.
    ```
    # The ordinals metaprotocol to index (brc-20, ltc-20, drc-20)
    META_PROTOCOL=ltc-20
    
    # Starting index block height
    START_BLOCK=2465225
    
    # How many blocks to stay behind to reduce the impact of block reorganisations (0 = disabled, 6 = recommended)
    BLOCKS_BEHIND=6
    
    # The ordinals server address (https://ordinals.com/, https://ordinalslite.com/, http://localhost/)
    ORDINALS_BASE_URL=http://localhost/
    
    # The postgres database credentials and url
    DATABASE_URL=postgres://postgres:password@localhost/ord20-indexer
    ```

9. Create database and then run the database installation scripts
    ```
    sudo -u postgres -p password createdb ord20-indexer
    diesel migration run
    ```

10. Run the ord-20 indexer
    ```
    ./target/release/ord20-indexer
    ```

<!-- ROADMAP -->
## Roadmap

- [ ] Extend the application to cover multiple ordinal meta protocols.
- [ ] Index directly from full nodes using the binary data found in blocks and transactions

See the [open issues](https://github.com/ynohtna92/ord20-indexer/issues) for a full list of proposed features (and known issues).

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<!-- LICENSE -->
## License

Distributed under the GNU v3 License. See `LICENSE.txt` for more information.

<!-- CONTACT -->
## Contact

Twitter/X - [@anthonyonchain](https://twitter.com/anthonyonchain)

Project Link: [https://github.com/ynohtna92/ord20-indexer.git](https://github.com/ynohtna92/ord20-indexer.git)

<p align="right">(<a href="#readme-top">back to top</a>)</p>
