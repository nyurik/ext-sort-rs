//! Loser tree merger.

use std::cmp::Ordering;

/// Former name of [`LoserTreeMerger`], which replaced the binary heap implementation.
#[deprecated(since = "0.2.0", note = "renamed to `LoserTreeMerger`")]
pub type BinaryHeapMerger<T, E, F, C> = LoserTreeMerger<T, E, F, C>;

/// Tournament tree over `k` inputs: `nodes[0]` is the input with the smallest head, `nodes[1..k]` the
/// losers of each match. Replacing the smallest head replays one match per level.
pub(crate) struct LoserTree {
    nodes: Vec<usize>,
}

impl LoserTree {
    /// Unset node, only seen while the tree is being built.
    const EMPTY: usize = usize::MAX;

    /// Builds the tree. `before(a, b)` tells whether the head of input `a` goes out before that of `b`;
    /// it must be a strict total order, so ties have to be broken, e.g. by input index.
    pub(crate) fn new(k: usize, before: impl Fn(usize, usize) -> bool) -> Self {
        let mut tree = LoserTree {
            nodes: vec![Self::EMPTY; k],
        };
        for leaf in 0..k {
            tree.replay(leaf, &before);
        }
        tree
    }

    /// Returns the input with the smallest head, or `None` if there are no inputs.
    #[inline]
    pub(crate) fn winner(&self) -> Option<usize> {
        self.nodes.first().copied()
    }

    /// Replays the matches on the path from `leaf` to the root after the head of `leaf` changed. While
    /// building, the first input to reach an unset node waits there for its opponent from the other subtree.
    #[inline]
    pub(crate) fn replay(&mut self, leaf: usize, before: impl Fn(usize, usize) -> bool) {
        // Leaves are virtual nodes `k..2k`, so the internal nodes `1..k` form a complete binary tree.
        let k = self.nodes.len();
        let mut winner = leaf;
        let mut node = (leaf + k) / 2;
        while node > 0 {
            let other = self.nodes[node];
            if other == Self::EMPTY {
                self.nodes[node] = winner;
                return;
            }
            if before(other, winner) {
                self.nodes[node] = winner;
                winner = other;
            }
            node /= 2;
        }
        self.nodes[0] = winner;
    }
}

/// Loser tree (tournament tree) merger.
/// Merges multiple sorted inputs into a single sorted output.
///
/// Replacing the smallest item costs one comparison per tree level, half of what a binary heap needs, and
/// items never move: the tree only stores input indexes. Time complexity is *m* \* log(*n*) where *m* is
/// the number of items and *n* the number of inputs.
///
/// Items that compare equal are returned in input order, so merging consecutive sorted runs of a stable
/// sort keeps the result stable.
///
/// When an input returns an error, the error is passed through and that input is dropped from the merge.
pub struct LoserTreeMerger<T, E, F, C>
where
    C: IntoIterator<Item = Result<T, E>>,
{
    chunks: Vec<C::IntoIter>,
    /// Current smallest item of each input, `None` once the input is exhausted.
    heads: Vec<Option<T>>,
    /// Built once all inputs are primed.
    tree: Option<LoserTree>,
    compare: F,
    /// Inputs are primed one at a time so that every error can be returned before merging starts.
    primed: usize,
    /// Input whose head was returned last. It advances on the next call, so an error reading it cannot
    /// swallow the item just taken from it.
    returned: Option<usize>,
}

impl<T, E, F, C> LoserTreeMerger<T, E, F, C>
where
    F: Fn(&T, &T) -> Ordering,
    C: IntoIterator<Item = Result<T, E>>,
{
    /// Creates a merger using chunks as inputs.
    /// Chunk items should be sorted in ascending order according to `compare`, otherwise the result is
    /// undefined.
    ///
    /// # Arguments
    /// * `chunks` - Chunks to be merged in a single sorted one
    /// * `compare` - Function used to compare items
    pub fn new<I>(chunks: I, compare: F) -> Self
    where
        I: IntoIterator<Item = C>,
    {
        let chunks: Vec<_> = chunks.into_iter().map(IntoIterator::into_iter).collect();
        LoserTreeMerger {
            heads: (0..chunks.len()).map(|_| None).collect(),
            chunks,
            tree: None,
            compare,
            primed: 0,
            returned: None,
        }
    }

    /// Returns the number of merged inputs.
    #[cfg(test)]
    pub(crate) fn inputs(&self) -> usize {
        self.chunks.len()
    }

    fn prime(&mut self) -> Result<(), E> {
        while self.primed < self.chunks.len() {
            let idx = self.primed;
            self.primed += 1;
            self.heads[idx] = self.chunks[idx].next().transpose()?;
        }
        Ok(())
    }
}

/// Whether the head of input `a` goes out before that of input `b`: exhausted inputs go last, and ties go
/// to the earlier input.
#[inline]
fn before<T>(heads: &[Option<T>], compare: impl Fn(&T, &T) -> Ordering, a: usize, b: usize) -> bool {
    match (&heads[a], &heads[b]) {
        (Some(x), Some(y)) => match compare(x, y) {
            Ordering::Less => true,
            Ordering::Equal => a < b,
            Ordering::Greater => false,
        },
        (x, _) => x.is_some(),
    }
}

impl<T, E, F, C> Iterator for LoserTreeMerger<T, E, F, C>
where
    F: Fn(&T, &T) -> Ordering,
    C: IntoIterator<Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    /// Returns the next item from the inputs in ascending order.
    fn next(&mut self) -> Option<Self::Item> {
        if self.tree.is_none() {
            if let Err(err) = self.prime() {
                return Some(Err(err));
            }
            let (heads, compare) = (&self.heads, &self.compare);
            self.tree = Some(LoserTree::new(heads.len(), |a, b| before(heads, compare, a, b)));
        }
        let tree = self.tree.as_mut()?;

        if let Some(last) = self.returned.take() {
            let result = self.chunks[last].next().transpose().map(|head| self.heads[last] = head);
            let (heads, compare) = (&self.heads, &self.compare);
            tree.replay(last, |a, b| before(heads, compare, a, b));
            if let Err(err) = result {
                return Some(Err(err));
            }
        }

        let winner = tree.winner()?;
        let item = self.heads[winner].take()?;
        self.returned = Some(winner);
        Some(Ok(item))
    }
}

#[cfg(test)]
mod test {
    use rand::Rng;
    use rstest::*;
    use std::error::Error;
    use std::io;

    use super::LoserTreeMerger;

    #[rstest]
    #[case(
        vec![],
        vec![],
    )]
    #[case(
        vec![
            vec![],
            vec![]
        ],
        vec![],
    )]
    #[case(
        vec![
            vec![Ok(4), Ok(5), Ok(7)],
            vec![Ok(1), Ok(6)],
            vec![Ok(3)],
            vec![],
        ],
        vec![Ok(1), Ok(3), Ok(4), Ok(5), Ok(6), Ok(7)],
    )]
    #[case(
        vec![
            vec![Result::Err(io::Error::other("test error"))]
        ],
        vec![
            Result::Err(io::Error::other("test error"))
        ],
    )]
    #[case(
        vec![
            vec![Ok(3), Result::Err(io::Error::other("test error"))],
            vec![Ok(1), Ok(2)],
        ],
        vec![
            Ok(1),
            Ok(2),
            Ok(3),
            Result::Err(io::Error::other("test error")),
        ],
    )]
    #[case(
        vec![
            vec![Ok(1), Ok(4)],
            vec![Result::Err(io::Error::other("test error"))],
            vec![Ok(2), Ok(3)],
        ],
        vec![
            Result::Err(io::Error::other("test error")),
            Ok(1),
            Ok(2),
            Ok(3),
            Ok(4),
        ],
    )]
    fn test_merger(
        #[case] chunks: Vec<Vec<Result<i32, io::Error>>>,
        #[case] expected_result: Vec<Result<i32, io::Error>>,
    ) {
        let merger = LoserTreeMerger::new(chunks, i32::cmp);
        let actual_result: Vec<_> = merger.collect();
        assert!(
            compare_vectors_of_result::<_, io::Error>(&actual_result, &expected_result),
            "actual={:?}, expected={:?}",
            actual_result,
            expected_result
        );
    }

    /// Every input width, including ones that leave the tree unbalanced, must give the stable sort order.
    #[test]
    fn test_merger_matches_stable_sort() {
        let mut rng = rand::thread_rng();
        for k in 1..=17 {
            // (value, chunk, position) with few distinct values, so most comparisons are ties
            let chunks: Vec<Vec<(u8, usize, usize)>> = (0..k)
                .map(|chunk| {
                    let mut values: Vec<u8> = (0..rng.gen_range(0..20)).map(|_| rng.gen_range(0..5)).collect();
                    values.sort_unstable();
                    values.into_iter().enumerate().map(|(pos, v)| (v, chunk, pos)).collect()
                })
                .collect();
            let mut expected = chunks.concat();
            expected.sort_by_key(|item| item.0);

            let inputs = chunks.into_iter().map(|c| c.into_iter().map(Ok::<_, io::Error>));
            let actual: Vec<_> = LoserTreeMerger::new(inputs, |a: &(u8, usize, usize), b| a.0.cmp(&b.0))
                .map(Result::unwrap)
                .collect();
            assert_eq!(actual, expected, "k={k}");
        }
    }

    fn compare_vectors_of_result<T: PartialEq, E: Error + 'static>(
        actual: &[Result<T, E>],
        expected: &[Result<T, E>],
    ) -> bool {
        actual.len() == expected.len()
            && actual.iter().zip(expected).all(|(actual_result, expected_result)| {
                match (actual_result, expected_result) {
                    (Ok(actual_result), Ok(expected_result)) if actual_result == expected_result => true,
                    (Err(actual_err), Err(expected_err)) => actual_err.to_string() == expected_err.to_string(),
                    _ => false,
                }
            })
    }
}
