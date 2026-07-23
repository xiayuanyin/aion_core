use aion_types::message::ImageInputCapability;
use serde_json::json;

use super::{
    IMAGE_INPUT_CATALOG_JSON, ImageInputCatalog, parse_catalog, resolve_from_catalog, resolve_image_input_capability,
};

fn catalog() -> ImageInputCatalog {
    serde_json::from_value(json!({
        "schema_version": 1,
        "providers": {
            "openai": {
                "models": ["gpt-4o"]
            },
            "google": {
                "models": ["gemini-2.5-flash"]
            },
            "amazon-bedrock": {
                "models": ["anthropic.claude-sonnet-4-20250514-v1:0"]
            },
            "dashscope": {
                "models": ["qwen3.7-plus"]
            },
            "moonshot-global": {
                "models": ["kimi-k2.6"]
            }
        }
    }))
    .expect("valid catalog fixture")
}

#[test]
fn embedded_allowlist_is_valid_and_contains_regression_models() {
    let catalog = parse_catalog(IMAGE_INPUT_CATALOG_JSON).expect("valid embedded catalog");

    assert_eq!(
        resolve_from_catalog(&catalog, "qwen3.7-plus"),
        ImageInputCapability::Supported
    );
    assert_eq!(
        resolve_from_catalog(&catalog, "kimi-k2.6"),
        ImageInputCapability::Supported
    );
}

#[test]
fn rejects_unknown_catalog_schema_version() {
    let error = parse_catalog(r#"{"schema_version":2,"providers":{"openai":{"models":["gpt-4o"]}}}"#)
        .expect_err("unknown schemas must fail closed");

    assert!(error.contains("unsupported catalog schema version 2"));
}

#[test]
fn embedded_allowlist_resolves_models_by_name_only() {
    assert_eq!(
        resolve_image_input_capability("qwen3.7-plus"),
        ImageInputCapability::Supported
    );
    assert_eq!(
        resolve_image_input_capability("kimi-k2.6"),
        ImageInputCapability::Supported
    );
}

#[test]
fn resolves_known_models_from_any_catalog_provider() {
    let catalog = catalog();

    assert_eq!(
        resolve_from_catalog(&catalog, "gpt-4o"),
        ImageInputCapability::Supported
    );
    assert_eq!(
        resolve_from_catalog(&catalog, "kimi-k2.6"),
        ImageInputCapability::Supported
    );
}

#[test]
fn normalizes_bedrock_inference_profile_prefixes() {
    let catalog = catalog();

    for model in [
        "anthropic.claude-sonnet-4-20250514-v1:0",
        "us.anthropic.claude-sonnet-4-20250514-v1:0",
        "global.anthropic.claude-sonnet-4-20250514-v1:0",
    ] {
        assert_eq!(resolve_from_catalog(&catalog, model), ImageInputCapability::Supported);
    }
}

#[test]
fn unknown_model_fails_closed_as_unknown() {
    let catalog = catalog();

    assert_eq!(
        resolve_from_catalog(&catalog, "missing-model"),
        ImageInputCapability::Unknown
    );
}
