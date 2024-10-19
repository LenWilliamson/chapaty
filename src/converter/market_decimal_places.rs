pub trait MyDecimalPlaces {
    fn round_to_n_decimal_places(self, n: i32) -> f64;
    fn round_nth_decimal_place_to_nearest_5_or_0(self, n: i32) -> f64;
}

impl MyDecimalPlaces for f64 {
    fn round_to_n_decimal_places(self, n: i32) -> Self {
        let x = 10.0_f64.powi(n);
        (self * x).round() / x
    }

    fn round_nth_decimal_place_to_nearest_5_or_0(self, n: i32) -> Self {
        let x = 10.0_f64.powi(n);
        let shifted = self * x;
        let rounded = shifted.round();
        let last_digit = (rounded % 10.0) as i32;
        let adjustment = match last_digit {
            1 | 2 => -last_digit,     // Round down to 0
            3 | 4 => 5 - last_digit,  // Round up to 5
            6 | 7 => 5 - last_digit,  // Round down to 5
            8 | 9 => 10 - last_digit, // Round up to 0
            _ => 0,                   // Already 0 or 5
        };
        ((rounded + adjustment as f64) / x).round_to_n_decimal_places(5)
    }
}

#[cfg(test)]
mod test {
    use crate::converter::market_decimal_places::MyDecimalPlaces;

    #[test]
    fn test_round_to_n_decimal_places() {
        let f = 1.1530499999999999;
        assert_eq!(1.15305, f.round_to_n_decimal_places(5))
    }

    #[test]
    fn test_round_nth_decimal_place_to_nearest_5_or_0() {
        let f = 1.1530499999999999;
        assert_eq!(1.15305, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.09603;
        assert_eq!(1.09605, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.08528;
        assert_eq!(1.0853, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.09959;
        assert_eq!(1.0996, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.08393;
        assert_eq!(1.08395, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.09389;
        assert_eq!(1.0939, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.09858;
        assert_eq!(1.0986, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.08736;
        assert_eq!(1.08735, f.round_nth_decimal_place_to_nearest_5_or_0(5));

        let f = 1.07768;
        assert_eq!(1.0777, f.round_nth_decimal_place_to_nearest_5_or_0(5));
    }
}
