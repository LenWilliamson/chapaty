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
