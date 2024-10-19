use std::marker::PhantomData;

#[macro_export]
macro_rules! compose {
    ({ $fh:expr } $( { $ft:expr } )*) => {
        crate::functools::ComposeableFn::new($fh)
        $( .compose($ft) )*
        .into_inner()
    }
}

/// A composable function type that allows for chaining multiple functions together.
/// Credits go to the author of [this answer](https://stackoverflow.com/questions/72597835/function-composition-chain-in-rust/72597950#72597950).
pub struct ComposeableFn<F, A, B>(F, PhantomData<*mut A>, PhantomData<*mut B>);

impl<F, A, B> ComposeableFn<F, A, B>
    where F: FnMut(A) -> B
{
    pub fn new(f: F) -> Self {
        Self(f, Default::default(), Default::default())
    }
    
    pub fn compose<C>(self, mut next: impl FnMut(B) -> C)
        -> ComposeableFn<impl FnMut(A) -> C, A, C>
    {
        let mut prior = self.0;
        
        ComposeableFn::new(move |v| next(prior(v)))
    }
    
    pub fn into_inner(self) -> F {
        self.0
    }
}