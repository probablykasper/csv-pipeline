use crate::RowResult;

pub type PipeIterator = Box<dyn Iterator<Item = RowResult>>;

pub struct Pipe {
	iterator: PipeIterator,
}
impl Pipe {
	pub fn new(iterator: PipeIterator) -> Self {
		Self {
			iterator: Box::new(iterator.into_iter()),
		}
	}
	pub fn with_state<S>(self, state: S) -> StatefulPipeBuilder<S> {
		StatefulPipeBuilder::new(self.iterator, state)
	}
}
impl Iterator for Pipe {
	type Item = RowResult;

	fn next(&mut self) -> Option<Self::Item> {
		self.iterator.next()
	}
}

pub struct StatefulPipeBuilder<S> {
	iterator: PipeIterator,
	state: S,
}
impl<S> StatefulPipeBuilder<S> {
	pub fn new(iterator: PipeIterator, state: S) -> Self {
		Self { state, iterator }
	}
	pub fn map<F>(self, f: F) -> StatefulPipe<S, F>
	where
		F: FnMut(RowResult, &mut S) -> RowResult,
	{
		StatefulPipe {
			iterator: self.iterator,
			state: self.state,
			f,
		}
	}
}

pub struct StatefulPipe<S, F: FnMut(RowResult, &mut S) -> RowResult> {
	pub(crate) iterator: PipeIterator,
	state: S,
	f: F,
}
impl<S, F> Iterator for StatefulPipe<S, F>
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
