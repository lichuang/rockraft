use std::sync::Arc;

use crate::error::{Error, Result};
use rocksdb::BlockBasedOptions;
use rocksdb::Cache;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::DBCompactionStyle;
use rocksdb::DBCompressionType;
use rocksdb::Options;
use rocksdb::SliceTransform;

#[derive(Debug, Clone)]
pub struct RocksDBConfig {
  pub block_cache_size: usize,
  pub write_buffer_size: usize,
  pub max_write_buffer_number: i32,
}

impl Default for RocksDBConfig {
  fn default() -> Self {
    Self {
      block_cache_size: 512 * 1024 * 1024,
      write_buffer_size: 128 * 1024 * 1024,
      max_write_buffer_number: 4,
    }
  }
}

#[derive(Debug)]
pub struct RocksDBEngine {
  db: Arc<DB>,
}

impl RocksDBEngine {
  /// Returns a shared reference to the underlying RocksDB instance.
  ///
  /// This is `pub(crate)` to prevent external crates from depending on
  /// the storage implementation detail.
  pub(crate) fn db(&self) -> &Arc<DB> {
    &self.db
  }
}

impl RocksDBEngine {
  pub fn new(data_path: &str, max_open_files: i32, cf_list: Vec<String>) -> Result<Self> {
    Self::new_with_config(data_path, max_open_files, None, cf_list)
  }

  pub fn new_with_config(
    data_path: &str,
    max_open_files: i32,
    config: Option<&RocksDBConfig>,
    cf_list: Vec<String>,
  ) -> Result<Self> {
    let default_config = RocksDBConfig::default();
    let cfg = config.unwrap_or(&default_config);

    let opts = Self::open_db_opts_with_config(max_open_files, cfg);
    let shared_cache = Cache::new_lru_cache(cfg.block_cache_size);
    let cf_column_family: Vec<_> = cf_list
      .into_iter()
      .map(|cf| {
        let cf_opts = Self::open_cf_opts_with_config(max_open_files, cfg, &shared_cache);
        ColumnFamilyDescriptor::new(cf, cf_opts)
      })
      .collect();

    let instance = DB::open_cf_descriptors(&opts, data_path, cf_column_family).map_err(|e| {
      Error::internal_with_source(format!("Failed to open RocksDB at {}", data_path), e)
    })?;

    Ok(RocksDBEngine {
      db: Arc::new(instance),
    })
  }

  /// Apply write buffer settings from config.
  fn apply_write_buffer_opts(opts: &mut Options, config: &RocksDBConfig) {
    opts.set_write_buffer_size(config.write_buffer_size);
    opts.set_max_write_buffer_number(config.max_write_buffer_number);
    opts.set_min_write_buffer_number_to_merge(2);
  }

  /// Apply shared compaction, compression, and prefix extractor settings.
  ///
  /// These tuning parameters are used by both the DB-level and column-family-level
  /// options, so any changes here affect all column families uniformly.
  fn apply_shared_tuning_opts(opts: &mut Options) {
    // Compaction
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_level_zero_file_num_compaction_trigger(8);
    opts.set_level_zero_stop_writes_trigger(32);
    opts.set_level_zero_slowdown_writes_trigger(16);
    opts.set_target_file_size_base(128 * 1024 * 1024);
    opts.set_target_file_size_multiplier(2);

    // Compression: Lz4 for hot levels, Zstd for cold levels
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_compression_per_level(&[
      DBCompressionType::None,
      DBCompressionType::None,
      DBCompressionType::Lz4,
      DBCompressionType::Lz4,
      DBCompressionType::Zstd,
    ]);

    // Prefix bloom for efficient range scans
    let transform = SliceTransform::create_fixed_prefix(10);
    opts.set_prefix_extractor(transform);
    opts.set_memtable_prefix_bloom_ratio(0.2);
  }

  /// Apply block-based table options with shared cache and bloom filters.
  fn apply_block_table_opts(opts: &mut Options, shared_cache: &Cache) {
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_bloom_filter(10.0, false);
    block_opts.set_block_size(4 * 1024);
    block_opts.set_block_cache(shared_cache);
    block_opts.set_cache_index_and_filter_blocks(true);
    block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_opts.set_index_type(rocksdb::BlockBasedIndexType::TwoLevelIndexSearch);
    block_opts.set_partition_filters(true);
    block_opts.set_whole_key_filtering(true);
    opts.set_block_based_table_factory(&block_opts);
  }

  fn open_db_opts_with_config(max_open_files: i32, config: &RocksDBConfig) -> Options {
    let mut opts = Options::default();

    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_max_open_files(max_open_files);

    Self::apply_write_buffer_opts(&mut opts, config);

    opts.set_enable_pipelined_write(true);
    opts.set_use_fsync(false);
    opts.set_disable_auto_compactions(false);
    opts.set_max_background_jobs(4);
    opts.set_max_subcompactions(2);
    opts.set_zstd_max_train_bytes(100 * 1024 * 1024);

    Self::apply_shared_tuning_opts(&mut opts);

    opts
  }

  fn open_cf_opts_with_config(
    _max_open_files: i32,
    config: &RocksDBConfig,
    shared_cache: &Cache,
  ) -> Options {
    let mut opts = Options::default();

    Self::apply_write_buffer_opts(&mut opts, config);
    Self::apply_shared_tuning_opts(&mut opts);
    Self::apply_block_table_opts(&mut opts, shared_cache);

    opts.set_max_bytes_for_level_base(256 * 1024 * 1024);
    opts.set_max_bytes_for_level_multiplier(10.0);

    opts
  }
}
