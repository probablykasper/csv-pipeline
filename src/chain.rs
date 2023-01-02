use crate::RowResult;
pub type BoxedIterator = Box<dyn Iterator<Item = RowResult>>;

/// A struct that wraps a RowResult iterator for convenience
pub struct Chain<C: Iterator<Item = RowResult>> {
	iterator: C,
}
impl<C: Iterator<Item = RowResult>> Chain<C> {
	pub fn new(iterator: C) -> Self {
		Self { iterator: iterator }
	}
	pub fn with_state<S>(self, state: S) -> StatefulChainBuilder<C, S> {
		StatefulChainBuilder::new(self.iterator, state)
	}
}
impl<C: Iterator<Item = RowResult>> Iterator for Chain<C> {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.iterator.next()
	}
}

pub struct StatefulChainBuilder<C: Iterator<Item = RowResult>, S> {
	iterator: C,
	state: S,
}
impl<C: Iterator<Item = RowResult>, S> StatefulChainBuilder<C, S> {
	pub fn new(iterator: C, state: S) -> Self {
		Self { state, iterator }
	}
	pub fn map<F>(self, f: F) -> StatefulChain<C, S, F>
	where
		F: FnMut(RowResult, &mut S) -> RowResult,
	{
		StatefulChain {
			iterator: self.iterator,
			state: self.state,
			f,
		}
	}
}

pub struct StatefulChain<C: Iterator<Item = RowResult>, S, F: FnMut(RowResult, &mut S) -> RowResult>
{
	iterator: C,
	state: S,
	f: F,
}
impl<C: Iterator<Item = RowResult>, S, F> StatefulChain<C, S, F>
where
	F: FnMut(RowResult, &mut S) -> RowResult,
{
	pub fn into_chain(self) -> Chain<StatefulChain<C, S, F>> {
		Chain::new(self)
	}
}
impl<C: Iterator<Item = RowResult>, S, F> Iterator for StatefulChain<C, S, F>
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
