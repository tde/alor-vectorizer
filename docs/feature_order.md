# Feature Vector Layout

The feature pipeline (`src/agragate/FeatureBuilder.js`) produces a fixed-order feature vector. The order is:

1. Scalar timing & microstructure
   - `scalar_dt_sec` (optional — only if `CONFIG.USE_RAW_DT`)
   - `scalar_log_dt`
   - `scalar_spread_ticks`
   - `scalar_spread_per_sec`
   - `scalar_spread_per_sec_z`
   - `scalar_microprice`
   - `scalar_micro_minus_mid`
   - `scalar_micro_delta_z`
   - `scalar_micro_speed`
   - `scalar_micro_speed_z`
2. Order book levels (per level `ℓ`, capped by `CONFIG.LEVELS_PER_SIDE`)
   - `bid_price_offset_ℓ`
   - `ask_price_offset_ℓ`
   - `bid_log_vol_ℓ`
   - `ask_log_vol_ℓ`
3. Depth/imbalance aggregates for K ∈ {1,3,5,10,20}
   - `imbalance_topK`, `imbalance_topK_z`
   - `cum_bid_topK_log`, `cum_ask_topK_log`
   - `depth_ratio_topK`, `depth_ratio_topK_z`
4. Shape descriptors
   - `bid_volume_slope`, `ask_volume_slope`
   - `bid_volume_convexity`, `ask_volume_convexity`
5. Book dynamics
   - Per-level liquidity change (K = `CONFIG.DVOL_LEVELS`): `bid_dvol_topℓ_z`, `ask_dvol_topℓ_z`
   - Aggregated flow for K ∈ {1,3,5}: `bid_dvol_sum_topK`, `ask_dvol_sum_topK`
   - Centers of gravity & speeds: `cog_bid`, `cog_ask`, `cog_bid_z`, `cog_ask_z`, `cog_bid_speed`, `cog_ask_speed`, `cog_bid_speed_z`, `cog_ask_speed_z`
6. Gap fragility signals
   - Zero-volume counters for windows {5,10}: `bid_zero_levels_topW`, `ask_zero_levels_topW`
   - Distance to the first “large” volume cluster: `bid_large_cluster_distance`, `ask_large_cluster_distance`
7. Trade-zone aggregates (for each configured price zone)
   - `zone_i_bid_count_z`, `zone_i_bid_vol_z`, `zone_i_bid_vwap_off_z`
   - `zone_i_ask_count_z`, `zone_i_ask_vol_z`, `zone_i_ask_vwap_off_z`
8. Trade-size bins (per `CONFIG.TRADE_SIZE_BINS`/`SIZE_BINS_KEEP`)
   - `size_bin_j_count_z`
   - `size_bin_j_volume_z`
9. Quantile trade bins (if history is long enough, `CONFIG.TRADE_QUANTILE_BINS` bins)
   - `trade_quantile_bin_j_count_z`
   - `trade_quantile_bin_j_volume_z`

The same ordering is exported together with the feature matrix (see `featureNames` in the build result). A human-readable list of the names for each run is written to `DATA_DIR/<symbol>_<date>_feature_names.txt` by the prepare module.
