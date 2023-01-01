use crate::RowResult;
pub type BoxedIterator = Box<dyn Iterator<Item = RowResult>>;

/// A struct that wraps a RowResult iterator for convenience
pub struct Chain {
	iterator: BoxedIterator,
}
impl Chain {
	pub fn new(iterator: BoxedIterator) -> Self {
		Self {
			iterator: Box::new(iterator),
		}
	}
	pub fn with_state<S>(self, state: S) -> StatefulChainBuilder<S> {
		StatefulChainBuilder::new(Box::new(self), state)
	}
}
impl Iterator for Chain {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.iterator.next()
	}
}

pub struct StatefulChainBuilder<S> {
	iterator: BoxedIterator,
	state: S,
}
impl<S> StatefulChainBuilder<S> {
	pub fn new(iterator: BoxedIterator, state: S) -> Self {
		Self { state, iterator }
	}
	pub fn map<F>(self, f: F) -> Chain
	where
		F: FnMut(RowResult, &mut S) -> RowResult,
	{
		let x = StatefulChain {
			iterator: self.iterator,
			state: self.state,
			f,
		};
		x.into_chain()
	}
}

pub struct StatefulChain<S, F: FnMut(RowResult, &mut S) -> RowResult> {
	iterator: BoxedIterator,
	state: S,
	f: F,
}
impl<S, F> StatefulChain<S, F>
where
	F: FnMut(RowResult, &mut S) -> RowResult,
{
	pub fn into_chain(self) -> Chain {
		Chain::new(Box::new(self))
	}
}
impl<S, F> Iterator for StatefulChain<S, F>
where
	F: FnMut(RowResult, &mut S) -> RowResult,
{
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		match self.iterator.next() {
			Some(item) => Some((self.f)(item, &mut self.state)),
			None => None,
		}
	}
}
