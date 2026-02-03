use crate::{
    data::{
        common::{MarketProfileStats, PocRule, ProfileBinStats, ValueAreaRule},
        domain::Price,
    },
    error::{ChapatyResult, DataError, SystemError},
};

pub fn compute_profile_stats<T: ProfileBinStats>(
    bins: &[T],
    va_pct: f64,
    poc_rule: PocRule,
    va_rule: ValueAreaRule,
) -> ChapatyResult<MarketProfileStats> {
    if bins.is_empty() {
        return Ok(MarketProfileStats {
            poc: Price(0.0),
            value_area_high: Price(0.0),
            value_area_low: Price(0.0),
        });
    }

    // 1. Identify Candidate POCs & Total Volume
    let mut max_vol = -1.0;
    let mut candidates = Vec::new();
    let mut total_vol = 0.0;

    for (i, bin) in bins.iter().enumerate() {
        let v = bin.get_value();
        total_vol += v;

        if v > max_vol {
            max_vol = v;
            candidates.clear();
            candidates.push(i);
        } else if (v - max_vol).abs() < f64::EPSILON {
            // Treat float equality carefully
            candidates.push(i);
        }
    }

    if candidates.is_empty() {
        return Err(DataError::NoEventsFound(
            "No volume candidates found for POC calculation".to_string(),
        )
        .into());
    }

    // 2. Disambiguate POC
    let poc_idx = match poc_rule {
        PocRule::LowestPrice => *candidates.first().ok_or_else(|| {
            SystemError::IndexOutOfBounds("POC candidates vector unexpectedly empty".to_string())
        })?,
        PocRule::HighestPrice => *candidates.last().ok_or_else(|| {
            SystemError::IndexOutOfBounds("POC candidates vector unexpectedly empty".to_string())
        })?,
        PocRule::ClosestToCenter => {
            if candidates.len() == 1 {
                candidates[0]
            } else {
                let sum_indices: usize = candidates.iter().sum();
                let avg_idx = sum_indices as f64 / candidates.len() as f64;
                *candidates
                    .iter()
                    .min_by(|&&a, &&b| {
                        let diff_a = (a as f64 - avg_idx).abs();
                        let diff_b = (b as f64 - avg_idx).abs();
                        // f64::total_cmp provides a total ordering where NaN is > Infinity
                        diff_a.total_cmp(&diff_b)
                    })
                    .ok_or_else(|| {
                        DataError::NoEventsFound(
                            "Failed to determine closest-to-center POC".to_string(),
                        )
                    })?
            }
        }
    };

    // 3. Calculate Value Area
    let target_vol = total_vol * va_pct;
    let mut current_vol = max_vol;
    let mut low_idx = poc_idx;
    let mut high_idx = poc_idx;

    while current_vol < target_vol {
        // Look at neighbors
        let vol_below = if low_idx > 0 {
            bins[low_idx - 1].get_value()
        } else {
            0.0
        };
        let vol_above = if high_idx < bins.len() - 1 {
            bins[high_idx + 1].get_value()
        } else {
            0.0
        };

        if vol_below == 0.0 && vol_above == 0.0 {
            break;
        }

        match va_rule {
            // Standard Steidlmayer (Favors Up on Tie)
            ValueAreaRule::HighestVolume => {
                if vol_above >= vol_below {
                    high_idx += 1;
                    current_vol += vol_above;
                } else {
                    low_idx -= 1;
                    current_vol += vol_below;
                }
            }
            // Favors Down on Tie
            ValueAreaRule::HighestVolumePreferLower => {
                if vol_above > vol_below {
                    high_idx += 1;
                    current_vol += vol_above;
                } else {
                    low_idx -= 1;
                    current_vol += vol_below;
                }
            }
            // Symmetric: Expand both sides simultaneously (if possible)
            ValueAreaRule::Symmetric => {
                // If both sides available, take both
                if vol_below > 0.0 && vol_above > 0.0 {
                    low_idx -= 1;
                    high_idx += 1;
                    current_vol += vol_below + vol_above;
                }
                // If only Up available
                else if vol_above > 0.0 {
                    high_idx += 1;
                    current_vol += vol_above;
                }
                // If only Down available
                else {
                    low_idx -= 1;
                    current_vol += vol_below;
                }
            }
        }
    }

    Ok(MarketProfileStats {
        poc: bins[poc_idx].get_price(),
        value_area_high: bins[high_idx].get_price(),
        value_area_low: bins[low_idx].get_price(),
    })
}

#[cfg(test)]
mod test {

    use super::*;

    // ============================================================================
    // Test Helpers (Mocking ProfileBinStats)
    // ============================================================================

    struct SimpleBin {
        price: f64,
        volume: f64,
    }

    impl ProfileBinStats for SimpleBin {
        fn get_price(&self) -> Price {
            Price(self.price)
        }
        fn get_value(&self) -> f64 {
            self.volume
        }
    }

    // Helper to build bins from explicit price/volume arrays
    fn make_bins(prices: &[f64], volumes: &[f64]) -> Vec<SimpleBin> {
        prices
            .iter()
            .zip(volumes.iter())
            .map(|(&p, &v)| SimpleBin {
                price: p,
                volume: v,
            })
            .collect()
    }

    // Helper to build bins with auto-incrementing prices (100.0, 101.0, ...)
    fn make_bins_auto_price(volumes: &[f64]) -> Vec<SimpleBin> {
        volumes
            .iter()
            .enumerate()
            .map(|(i, &v)| SimpleBin {
                price: 100.0 + i as f64,
                volume: v,
            })
            .collect()
    }

    // ============================================================================
    // POC Rules
    // ============================================================================

    #[test]
    fn test_poc_single_clear_winner() {
        // [10, 50, 10] -> POC is index 1
        let bins = make_bins(&[100.0, 101.0, 102.0], &[10.0, 50.0, 10.0]);

        // Default rules: LowestPrice / HighestVolume
        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(101.0));
    }

    #[test]
    fn test_poc_rule_lowest_price() {
        // Bimodal: [50, 10, 50]. Max vol is 50 at indices 0 and 2.
        // LowestPrice -> Index 0 (100.0)
        let bins = make_bins(&[100.0, 101.0, 102.0], &[50.0, 10.0, 50.0]);

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(100.0));
    }

    #[test]
    fn test_poc_rule_highest_price() {
        // Bimodal: [50, 10, 50].
        // HighestPrice -> Index 2 (102.0)
        let bins = make_bins(&[100.0, 101.0, 102.0], &[50.0, 10.0, 50.0]);

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::HighestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(102.0));
    }

    #[test]
    fn test_poc_rule_closest_to_center_odd() {
        let bins = make_bins(
            &[100.0, 101.0, 102.0, 103.0, 104.0],
            &[10.0, 50.0, 50.0, 50.0, 10.0],
        );

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::ClosestToCenter,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(102.0));
    }

    #[test]
    fn test_poc_floating_point_precision() {
        // Ensure strictly that 0.30000000000000004 isn't treated differently than 0.3
        // `if (v - max_vol).abs() < f64::EPSILON`

        let v1 = 0.1 + 0.2; // 0.30000000000000004
        let v2 = 0.3;
        let v3 = 0.1 + 0.1 + 0.1;

        let bins = make_bins(&[100.0, 100.5, 101.0], &[v1, v3, v2]);

        // 1. Highest Price Rule
        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::HighestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        // With Epsilon check, v1 and v2 and v3 are "Equal Max". HighestPrice rule takes the last one (101.0).
        assert_eq!(
            res.poc,
            Price(101.0),
            "Epsilon check failed to equate 0.1+0.2, 0.1+0.1+0.1 and 0.3"
        );

        // 2. Lowest Price Rule
        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        // With Epsilon check, v1 and v2 and v3 are "Equal Max". LowestPrice rule takes the first one (100.0).
        assert_eq!(
            res.poc,
            Price(100.0),
            "Epsilon check failed to equate 0.1+0.2, 0.1+0.1+0.1 and 0.3"
        );

        // 3. Closest To Center Rule
        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::ClosestToCenter,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        // With Epsilon check, v1 and v2 and v3 are "Equal Max". ClosestToCenter rule takes the middle one (100.5).
        assert_eq!(
            res.poc,
            Price(100.5),
            "Epsilon check failed to equate 0.1+0.2, 0.1+0.1+0.1 and 0.3"
        );
    }

    // ============================================================================
    // VA Rules
    // ============================================================================

    #[test]
    fn test_va_standard_highest_volume() {
        // SCENARIO: Normal distribution-ish.
        // Vals: [5, 10, 50, 20, 15]
        // Total: 100. Target 70%: 70.
        // POC: Index 2 (Val 50). Current = 50.
        // Needs 20 more.
        // Neighbors: Up=20 (idx 3), Down=10 (idx 1).
        // Rule: HighestVolume. 20 > 10. Pick Up.
        // Current: 50 + 20 = 70.
        // Target met. Stop.
        // Result: POC=102, Low=102, High=103.

        let bins = make_bins_auto_price(&[5.0, 10.0, 50.0, 20.0, 15.0]);

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(102.0));
        assert_eq!(res.value_area_low, Price(102.0));
        assert_eq!(res.value_area_high, Price(103.0));
    }

    #[test]
    fn test_va_symmetric_expansion() {
        // SCENARIO: Symmetric rule expands both ways if data exists.
        // Vals: [10, 10, 40, 10, 30]
        // Total: 100. Target 70.
        // POC: Index 2 (40).
        // Step 1: Look neighbors (10 and 10). Both > 0.
        // Symmetric takes BOTH. Current = 40 + 10 + 10 = 60. Range [101, 103].
        // Step 2: Look neighbors (10 and 30). Both > 0.
        // Symmetric takes BOTH. Current = 60 + 10 + 30 = 100. Range [100, 104].
        // Stop.

        let prices: Vec<f64> = vec![100.0, 101.0, 102.0, 103.0, 104.0];
        let vols: Vec<f64> = vec![10.0, 10.0, 40.0, 10.0, 30.0];
        let bins = make_bins(&prices, &vols);

        let res = compute_profile_stats(&bins, 0.7, PocRule::LowestPrice, ValueAreaRule::Symmetric)
            .expect("failed to compute stats");

        assert_eq!(res.value_area_low, Price(100.0));
        assert_eq!(res.value_area_high, Price(104.0));
    }

    #[test]
    fn test_va_fat_poc_immediate_saturation() {
        // SCENARIO: The POC itself contains > 70% of volume.
        // Should not expand at all.
        // Vals: [5, 80, 15]. Total 100. Target 70.
        // POC: 80. 80 > 70. Done immediately.

        let bins = make_bins_auto_price(&[5.0, 80.0, 15.0]);

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(101.0));
        assert_eq!(res.value_area_low, Price(101.0));
        assert_eq!(res.value_area_high, Price(101.0));
    }

    #[test]
    fn test_va_boundary_constraints() {
        // SCENARIO: POC is at index 0. Can only expand UP.
        // Vals: [60, 20, 20]. Total 100. Target 70.
        // POC: Index 0 (60).
        // Iter 1: Down=0 (OOB), Up=20.
        // Must take Up. Current=80. Stop.
        // Result: Low=100, High=101.

        let bins = make_bins_auto_price(&[60.0, 20.0, 20.0]);

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolume,
        )
        .expect("failed to compute stats");

        assert_eq!(res.poc, Price(100.0));
        assert_eq!(res.value_area_low, Price(100.0));
        assert_eq!(res.value_area_high, Price(101.0));
    }

    #[test]
    fn test_va_tie_breaker_prefer_lower() {
        // SCENARIO: Neighbors have equal volume.
        // Rule: HighestVolumePreferLower.
        // Vals: [10, 10, 50, 10, 10]
        // POC: Index 2 (50).
        // Neighbors: Up=10, Down=10. Tie!
        // PreferLower -> Pick Down (Index 1).
        // Current = 60.
        // Next Neighbors: Up=10 (Index 3), Down=10 (Index 0). Tie!
        // PreferLower -> Pick Down (Index 0).
        // Current = 70. Stop.
        // Result: Low=100, High=102. (Did not touch 103).

        let prices: Vec<f64> = vec![100.0, 101.0, 102.0, 103.0, 104.0];
        let vols: Vec<f64> = vec![10.0, 10.0, 50.0, 10.0, 10.0];
        let bins = make_bins(&prices, &vols);

        let res = compute_profile_stats(
            &bins,
            0.7,
            PocRule::LowestPrice,
            ValueAreaRule::HighestVolumePreferLower,
        )
        .expect("failed to compute stats");

        assert_eq!(res.value_area_low, Price(100.0));
        assert_eq!(res.value_area_high, Price(102.0));
    }
}
