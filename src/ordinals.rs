use crate::checkpoint::{valid_hash, CheckResult};
use serde::Deserialize;
use std::time::Duration;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Inscriptions {
    pub inscriptions: Vec<InscriptionItem>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct InscriptionItem {
    pub href: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Inscription {
    pub number: i64,
    pub offset: i64,
    pub genesis_fee: i64,
    pub genesis_height: i64,
    pub genesis_transaction: String,
    pub inscription_id: String,
    pub output: String,
    pub location: String,
    pub address: String,
    pub genesis_address: String,
    pub content_type: Option<String>,
    pub timestamp: String,
}

#[derive(Debug, Deserialize)]
pub struct TransactionInscription {
    pub inscription_id: String,
    pub inscription_number: i64,
    pub content_type: Option<String>,
    pub content: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Transaction {
    pub transaction: String,
    pub inputs: Vec<(String, u64)>,
    pub outputs: Vec<(String, u64)>,
    pub output_addresses: Vec<String>,
    pub inscriptions: Vec<TransactionInscription>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Block {
    pub hash: String,
    pub height: i32,
    pub previous_blockhash: String,
    pub size: i32,
    pub target: String,
    pub timestamp: String,
    pub transactions: Vec<Transaction>,
    pub weight: i32,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub struct Output {
    pub address: String,
    pub transaction: String,
    pub value: u64,
}

#[derive(Clone)]
pub struct Ordinals {
    base_url: String,
    client: reqwest::Client,
}

impl Ordinals {
    pub(crate) fn new(base_url: String) -> Self {
        Ordinals {
            base_url: format!("{}/", base_url.trim_end_matches('/')),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .expect("Cannot create Ord client"),
        }
    }

    pub(crate) async fn verify_chain(&self, minimum_height: i32) -> CheckResult<()> {
        let value: serde_json::Value = self
            .client
            .get(format!("{}status", self.base_url))
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| "Ord status unavailable")?
            .error_for_status()
            .map_err(|_| "Ord status unavailable")?
            .json()
            .await
            .map_err(|_| "Invalid Ord status")?;
        if value["chain"].as_str() != Some("mainnet")
            || value["unrecoverably_reorged"].as_bool() != Some(false)
            || value["height"]
                .as_i64()
                .is_none_or(|height| height < minimum_height as i64)
            || value["inscriptions"]
                .as_u64()
                .is_none_or(|count| count == 0)
            || value["inscription_index"].as_bool() == Some(false)
        {
            return Err("Ord is not a healthy mainnet inscription index".into());
        }
        Ok(())
    }

    pub(crate) async fn get_block_hash(&self, height: i32) -> CheckResult<String> {
        let value = self
            .client
            .get(format!("{}blockhash/{}", self.base_url, height))
            .send()
            .await
            .map_err(|_| "Canonical hash unavailable")?
            .error_for_status()
            .map_err(|_| "Canonical hash unavailable")?
            .text()
            .await
            .map_err(|_| "Invalid canonical hash")?;
        let hash = value.trim().to_string();
        if !valid_hash(&hash) {
            return Err("Invalid canonical block hash".into());
        }
        Ok(hash)
    }

    #[allow(unused)]
    pub(crate) async fn get_status(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        let url = format!("{}status", self.base_url);
        let response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let content = response.text().await?;
            let status = content.trim().to_string();
            Ok(status)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }

    pub(crate) async fn get_block_height(&mut self) -> Result<i32, Box<dyn std::error::Error>> {
        let url = format!("{}blockheight", self.base_url);
        let response = self.client.get(url).send().await?;

        if response.status().is_success() {
            let content = response.text().await?;
            let block_height = content.trim().parse::<i32>()?;
            Ok(block_height)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }

    pub(crate) async fn get_block(
        &mut self,
        block_height: i32,
    ) -> Result<Block, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}block/{}", self.base_url, block_height);
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let block = response.json::<Block>().await?;
            Ok(block)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }

    pub(crate) async fn get_output(
        &mut self,
        outpoint: String,
    ) -> Result<Output, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}output/{}", self.base_url, outpoint);
        let response = self
            .client
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let output = response.json::<Output>().await?;
            Ok(output)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }

    #[allow(unused)]
    pub(crate) async fn get_inscriptions(
        &mut self,
        inscription_num: i64,
    ) -> Result<Inscriptions, Box<dyn std::error::Error>> {
        let url = format!("{}inscriptions/{}", self.base_url, inscription_num);
        let response = reqwest::Client::new()
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let inscriptions = response.json::<Inscriptions>().await?;
            Ok(inscriptions)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }

    #[allow(unused)]
    pub(crate) async fn get_inscription(
        &mut self,
        inscription_id: String,
    ) -> Result<Inscription, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}inscription/{}", self.base_url, inscription_id.as_str());
        println!("get_inscription: {}", url);

        let response = reqwest::Client::new()
            .get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?;

        if response.status().is_success() {
            let inscription = response.json::<Inscription>().await?;
            Ok(inscription)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }

    #[allow(unused)]
    pub(crate) async fn get_inscription_content(
        &mut self,
        inscription_id: String,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}content/{}", self.base_url, inscription_id.as_str());
        println!("{:?}", url);
        let response = reqwest::get(url).await?;

        if response.status().is_success() {
            let content = response.text().await?;
            Ok(content)
        } else {
            Err(format!("Failed with status code: {}", response.status()).into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ordinals::TransactionInscription;

    #[test]
    fn test_transaction_inscription_deserialization() {
        let json_data = "
        {
            \"inscription_id\": \"71e0f6dc87a473aa69787fff8e09e5eddfdca96e587928a5b1a25c0ae16dc0eei0\",
            \"inscription_number\": 0,
            \"content_type\": \"text/plain\",
            \"content\": \"hello world\"
        }";

        let result: Result<TransactionInscription, serde_json::Error> =
            serde_json::from_str(json_data);

        assert!(result.is_ok());

        let transaction_inscription = result.unwrap();

        assert_eq!(
            transaction_inscription.inscription_id,
            "71e0f6dc87a473aa69787fff8e09e5eddfdca96e587928a5b1a25c0ae16dc0eei0"
        );
        assert_eq!(transaction_inscription.inscription_number, 0);
        assert_eq!(
            transaction_inscription.content_type,
            Some("text/plain".to_string())
        );
        assert_eq!(
            transaction_inscription.content,
            Some("hello world".to_string())
        );
    }

    #[test]
    fn test_transaction_inscription_null_content_deserialization() {
        let json_data = "
        {
            \"inscription_id\": \"71e0f6dc87a473aa69787fff8e09e5eddfdca96e587928a5b1a25c0ae16dc0eei0\",
            \"inscription_number\": 0,
            \"content_type\": null,
            \"content\": null
        }";

        let result: Result<TransactionInscription, serde_json::Error> =
            serde_json::from_str(json_data);

        assert!(result.is_ok());

        let transaction_inscription = result.unwrap();

        assert_eq!(
            transaction_inscription.inscription_id,
            "71e0f6dc87a473aa69787fff8e09e5eddfdca96e587928a5b1a25c0ae16dc0eei0"
        );
        assert_eq!(transaction_inscription.inscription_number, 0);
        assert_eq!(transaction_inscription.content_type, None);
        assert_eq!(transaction_inscription.content, None);
    }
}
