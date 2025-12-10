pub const LOG_META_FAMILY: &str = "_log_meta";
pub const LOG_DATA_FAMILY: &str = "_log_data";
pub const SM_META_FAMILY: &str = "_sm_meta";
pub const SM_DATA_FAMILY: &str = "_sm_data";

pub const LAST_APPLIED_LOG_KEY: &str = "last_applied_log";
pub const LAST_MEMBERSHIP_KEY: &str = "last_membership";

pub fn column_family_list() -> Vec<String> {
  vec![
    LOG_META_FAMILY.to_string(),
    LOG_DATA_FAMILY.to_string(),
    SM_META_FAMILY.to_string(),
    SM_DATA_FAMILY.to_string(),
  ]
}
