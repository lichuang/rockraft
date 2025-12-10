pub const LOG_META_FAMILY: &str = "_log_meta";
pub const LOG_DATA_FAMILY: &str = "_log_data";

pub fn column_family_list() -> Vec<String> {
  vec![LOG_META_FAMILY.to_string(), LOG_DATA_FAMILY.to_string()]
}
