use std::sync::atomic::{AtomicBool, Ordering};

use crate::AgentType;

pub const EXTERNAL_ACP_DISABLED_MESSAGE: &str = "External ACP runtimes are disabled by the host application.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExternalAcpPolicy {
    Enabled,
    Disabled,
}

impl ExternalAcpPolicy {
    pub fn allows(self, agent_type: AgentType) -> bool {
        agent_type != AgentType::Acp || self == Self::Enabled
    }
}

static EXTERNAL_ACP_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_external_acp_policy(policy: ExternalAcpPolicy) {
    EXTERNAL_ACP_ENABLED.store(policy == ExternalAcpPolicy::Enabled, Ordering::Release);
}

pub fn external_acp_policy() -> ExternalAcpPolicy {
    if EXTERNAL_ACP_ENABLED.load(Ordering::Acquire) {
        ExternalAcpPolicy::Enabled
    } else {
        ExternalAcpPolicy::Disabled
    }
}

pub fn host_allows_agent_type(agent_type: AgentType) -> bool {
    external_acp_policy().allows(agent_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_policy_rejects_only_external_acp() {
        assert!(!ExternalAcpPolicy::Disabled.allows(AgentType::Acp));
        assert!(ExternalAcpPolicy::Disabled.allows(AgentType::Aionrs));
    }
}
