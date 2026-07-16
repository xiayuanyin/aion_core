use std::collections::{BTreeMap, HashMap};

use aionui_common::EnvVar;
use aionui_db::IClientPreferenceRepository;
use tracing::warn;

use crate::registry::is_blocked_override_env_key;

const PROFILE_INJECT_ENVS_KEY: &str = "profile.injectEnvs";
const SYSTEM_INJECT_ENVS_KEY: &str = "system.injectEnvs";

pub(super) async fn apply_injected_envs(repo: &dyn IClientPreferenceRepository, env: &mut Vec<EnvVar>) {
    let injected = load_injected_envs(repo).await;
    let mut merged = injected
        .into_iter()
        .map(|entry| (entry.name, entry.value))
        .collect::<BTreeMap<_, _>>();
    for entry in env.iter() {
        merged.insert(entry.name.clone(), entry.value.clone());
    }
    *env = merged.into_iter().map(|(name, value)| EnvVar { name, value }).collect();
}

pub(super) async fn load_injected_envs(repo: &dyn IClientPreferenceRepository) -> Vec<EnvVar> {
    let rows = match repo
        .get_by_keys(&[PROFILE_INJECT_ENVS_KEY, SYSTEM_INJECT_ENVS_KEY])
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            warn!(%error, "Failed to load injected environment variables");
            return Vec::new();
        }
    };

    let mut by_key = rows
        .into_iter()
        .map(|row| (row.key, row.value))
        .collect::<HashMap<_, _>>();
    let profile = parse_env_record(by_key.remove(PROFILE_INJECT_ENVS_KEY).as_deref());
    let system = parse_env_record(by_key.remove(SYSTEM_INJECT_ENVS_KEY).as_deref());
    merge_envs(profile, system)
}

fn parse_env_record(raw: Option<&str>) -> BTreeMap<String, String> {
    let Some(raw) = raw else {
        return BTreeMap::new();
    };
    let Ok(values) = serde_json::from_str::<HashMap<String, String>>(raw) else {
        warn!("Ignoring malformed injected environment variable settings");
        return BTreeMap::new();
    };

    values
        .into_iter()
        .filter(|(key, _)| is_valid_env_key(key) && !is_blocked_override_env_key(key))
        .collect()
}

fn merge_envs(profile: BTreeMap<String, String>, system: BTreeMap<String, String>) -> Vec<EnvVar> {
    let mut merged = profile;
    merged.extend(system);
    merged.into_iter().map(|(name, value)| EnvVar { name, value }).collect()
}

fn is_valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_db::{IClientPreferenceRepository, SqliteClientPreferenceRepository, init_database_memory};

    #[tokio::test]
    async fn loads_and_merges_injected_envs_with_expected_precedence() {
        let database = init_database_memory().await.unwrap();
        let repo = SqliteClientPreferenceRepository::new(database.pool().clone());
        repo.upsert_batch(&[
            (
                PROFILE_INJECT_ENVS_KEY,
                r#"{"PROFILE_ONLY":"profile","SHARED":"profile","PATH":"/blocked"}"#,
            ),
            (
                SYSTEM_INJECT_ENVS_KEY,
                r#"{"MANUAL_ONLY":"manual","SHARED":"manual","INVALID-KEY":"ignored"}"#,
            ),
        ])
        .await
        .unwrap();
        let mut env = vec![EnvVar {
            name: "SHARED".to_owned(),
            value: "agent".to_owned(),
        }];

        apply_injected_envs(&repo, &mut env).await;

        let env = env
            .into_iter()
            .map(|entry| (entry.name, entry.value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(env.get("PROFILE_ONLY").map(String::as_str), Some("profile"));
        assert_eq!(env.get("MANUAL_ONLY").map(String::as_str), Some("manual"));
        assert_eq!(env.get("SHARED").map(String::as_str), Some("agent"));
        assert!(!env.contains_key("PATH"));
        assert!(!env.contains_key("INVALID-KEY"));
    }

    #[test]
    fn manual_env_overrides_profile_env() {
        let env = merge_envs(
            BTreeMap::from([("KEY".to_owned(), "profile".to_owned())]),
            BTreeMap::from([("KEY".to_owned(), "manual".to_owned())]),
        );

        assert_eq!(
            env,
            vec![EnvVar {
                name: "KEY".to_owned(),
                value: "manual".to_owned(),
            }]
        );
    }
}
