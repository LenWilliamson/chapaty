pub trait MyDecimalPlaces {
    fn round_to_n_decimal_places(self, n: i32) -> f64;
    fn round_to_dollar_cents(self) -> f64;
}

impl MyDecimalPlaces for f64 {
    fn round_to_n_decimal_places(self, n: i32) -> Self {
        let x = 10.0_f64.powi(n);
        (self * x).round() / x
    }

    fn round_to_dollar_cents(self) -> Self {
        let x: f64 = 100.0;
        (self * x).round() / x
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
}