/// Macro to implement `From<T>` for newtype wrappers around primitive types.
#[macro_export]
macro_rules! impl_from_primitive {
    ($wrapper:ident, $primitive:ty) => {
        impl From<$primitive> for $wrapper {
            fn from(value: $primitive) -> Self {
                Self(value)
            }
        }

        impl From<$wrapper> for $primitive {
            fn from(wrapper: $wrapper) -> Self {
                wrapper.0
            }
        }
    };
}

/// Macro to implement `Add`, `Sub`, `Mul`, `Div`, and `Sum` traits for newtype wrappers around numeric types,
/// including support for adding a primitive type directly.
#[macro_export]
macro_rules! impl_add_sub_mul_div_primitive {
    ($wrapper:ident, $primitive:ty) => {
        impl std::ops::Add for $wrapper {
            type Output = Self;

            fn add(self, other: Self) -> Self {
                Self(self.0 + other.0)
            }
        }

        impl std::ops::Add<$primitive> for $wrapper {
            type Output = Self;

            fn add(self, rhs: $primitive) -> Self::Output {
                Self(self.0 + rhs)
            }
        }

        impl std::ops::AddAssign<$primitive> for $wrapper {
            fn add_assign(&mut self, rhs: $primitive) {
                self.0 += rhs;
            }
        }

        impl std::ops::Sub for $wrapper {
            type Output = Self;

            fn sub(self, other: Self) -> Self {
                Self(self.0 - other.0)
            }
        }

        impl std::ops::Mul for $wrapper {
            type Output = Self;

            fn mul(self, other: Self) -> Self {
                Self(self.0 * other.0)
            }
        }

        impl std::ops::Div for $wrapper {
            type Output = Self;

            fn div(self, other: Self) -> Self {
                Self(self.0 / other.0)
            }
        }

        impl std::iter::Sum for $wrapper {
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                iter.fold(Self(0 as $primitive), |acc, x| Self(acc.0 + x.0))
            }
        }
    };
}

/// Macro to implement an `abs` method for newtype wrappers around numeric types.
#[macro_export]
macro_rules! impl_abs_primitive {
    ($wrapper:ident, $primitive:ty) => {
        impl $wrapper {
            /// Returns the absolute value of the wrapped primitive.
            pub fn abs(self) -> Self {
                Self(self.0.abs())
            }
        }
    };
}

/// Macro to implement the `Neg` trait for newtype wrappers around numeric types.
#[macro_export]
macro_rules! impl_neg_primitive {
    ($wrapper:ident, $primitive:ty) => {
        impl std::ops::Neg for $wrapper {
            type Output = Self;

            fn neg(self) -> Self::Output {
                Self(-self.0)
            }
        }
    };
}
