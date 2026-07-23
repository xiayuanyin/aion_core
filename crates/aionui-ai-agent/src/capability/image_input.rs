use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use aion_types::message::ImageInputCapability;
use serde::Deserialize;
use tracing::error;

const IMAGE_INPUT_CATALOG_SCHEMA_VERSION: u32 = 1;
const IMAGE_INPUT_CATALOG_JSON: &str = include_str!("../../assets/model-capabilities/image_input_models.json");
const BEDROCK_INFERENCE_PROFILE_PREFIXES: [&str; 6] = ["us.", "eu.", "apac.", "au.", "jp.", "global."];

static IMAGE_INPUT_CATALOG: OnceLock<Option<ImageInputCatalog>> = OnceLock::new();

#[derive(Debug, Deserialize)]
struct ImageInputCatalog {
    schema_version: u32,
    providers: HashMap<String, ImageInputProvider>,
}

#[derive(Debug, Deserialize)]
struct ImageInputProvider {
    #[serde(default)]
    models: HashSet<String>,
}

pub(crate) fn resolve_image_input_capability(model: &str) -> ImageInputCapability {
    embedded_catalog()
        .map(|catalog| resolve_from_catalog(catalog, model))
        .unwrap_or(ImageInputCapability::Unknown)
}

fn embedded_catalog() -> Option<&'static ImageInputCatalog> {
    IMAGE_INPUT_CATALOG
        .get_or_init(|| match parse_catalog(IMAGE_INPUT_CATALOG_JSON) {
            Ok(catalog) => Some(catalog),
            Err(parse_error) => {
                error!(error = %parse_error, "Failed to parse embedded image input model catalog");
                None
            }
        })
        .as_ref()
}

fn parse_catalog(json: &str) -> Result<ImageInputCatalog, String> {
    let catalog = serde_json::from_str::<ImageInputCatalog>(json)
        .map_err(|parse_error| format!("invalid catalog JSON: {parse_error}"))?;
    if catalog.schema_version != IMAGE_INPUT_CATALOG_SCHEMA_VERSION {
        return Err(format!(
            "unsupported catalog schema version {}; expected {IMAGE_INPUT_CATALOG_SCHEMA_VERSION}",
            catalog.schema_version
        ));
    }
    if catalog.providers.is_empty() {
        return Err("catalog contains no providers".to_owned());
    }
    Ok(catalog)
}

fn resolve_from_catalog(catalog: &ImageInputCatalog, model: &str) -> ImageInputCapability {
    if catalog
        .providers
        .values()
        .any(|provider| model_supports_image(provider, model))
    {
        ImageInputCapability::Supported
    } else {
        ImageInputCapability::Unknown
    }
}

fn model_supports_image(provider: &ImageInputProvider, model: &str) -> bool {
    provider.models.contains(normalize_model_id(model))
}

fn normalize_model_id(model: &str) -> &str {
    let model = model.strip_prefix("models/").unwrap_or(model);
    BEDROCK_INFERENCE_PROFILE_PREFIXES
        .iter()
        .find_map(|prefix| {
            model
                .strip_prefix(prefix)
                .filter(|model| model.starts_with("anthropic."))
        })
        .unwrap_or(model)
}

#[cfg(test)]
#[path = "image_input_test.rs"]
mod image_input_test;
