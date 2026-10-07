//! Radix sortable keys.

/// A fixed-size sort key that can be radix sorted byte by byte, used by
/// [`RecordSorter`](crate::record::RecordSorter).
///
/// Implemented for unsigned and signed integers, `bool`, `char`, byte arrays (compared lexicographically)
/// and pairs and triples of keys (compared lexicographically).
pub trait RadixKey: Copy + Ord + Send + Sync + 'static {
    /// Number of bytes (radix digits) of the key, and of its encoding.
    const BYTES: usize;

    /// Returns key byte `digit` in `0..BYTES`, the least significant first.
    ///
    /// Comparing two keys by their bytes from the most significant to the least significant must give the
    /// same result as [`Ord`].
    fn digit(&self, digit: usize) -> u8;

    /// Appends the `BYTES` long encoding of the key to `out`.
    fn write(&self, out: &mut Vec<u8>);

    /// Decodes a key from the `BYTES` long encoding [`RadixKey::write`] produced, or returns `None` if it
    /// is not a valid encoding.
    fn read(bytes: &[u8]) -> Option<Self>;
}

macro_rules! radix_key_int {
    ($($t:ty => $u:ty, $flip:expr;)*) => {$(
        impl RadixKey for $t {
            const BYTES: usize = std::mem::size_of::<$t>();

            #[inline]
            fn digit(&self, digit: usize) -> u8 {
                // Flipping the sign bit of signed keys maps their order onto the unsigned one.
                ((*self as $u ^ $flip) >> (digit * 8)) as u8
            }

            #[inline]
            fn write(&self, out: &mut Vec<u8>) {
                out.extend_from_slice(&self.to_le_bytes());
            }

            #[inline]
            fn read(bytes: &[u8]) -> Option<Self> {
                Some(<$t>::from_le_bytes(bytes.try_into().ok()?))
            }
        }
    )*};
}

radix_key_int! {
    u8 => u8, 0;
    u16 => u16, 0;
    u32 => u32, 0;
    u64 => u64, 0;
    u128 => u128, 0;
    usize => usize, 0;
    i8 => u8, 1 << 7;
    i16 => u16, 1 << 15;
    i32 => u32, 1 << 31;
    i64 => u64, 1 << 63;
    i128 => u128, 1 << 127;
    isize => usize, 1 << (usize::BITS - 1);
}

impl RadixKey for bool {
    const BYTES: usize = 1;

    #[inline]
    fn digit(&self, _digit: usize) -> u8 {
        u8::from(*self)
    }

    #[inline]
    fn write(&self, out: &mut Vec<u8>) {
        out.push(u8::from(*self));
    }

    #[inline]
    fn read(bytes: &[u8]) -> Option<Self> {
        match bytes {
            [0] => Some(false),
            [1] => Some(true),
            _ => None,
        }
    }
}

impl RadixKey for char {
    const BYTES: usize = 4;

    #[inline]
    fn digit(&self, digit: usize) -> u8 {
        u32::from(*self).digit(digit)
    }

    #[inline]
    fn write(&self, out: &mut Vec<u8>) {
        u32::from(*self).write(out);
    }

    #[inline]
    fn read(bytes: &[u8]) -> Option<Self> {
        char::from_u32(u32::read(bytes)?)
    }
}

impl<const N: usize> RadixKey for [u8; N] {
    const BYTES: usize = N;

    #[inline]
    fn digit(&self, digit: usize) -> u8 {
        self[N - 1 - digit]
    }

    #[inline]
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }

    #[inline]
    fn read(bytes: &[u8]) -> Option<Self> {
        bytes.try_into().ok()
    }
}

macro_rules! radix_key_tuple {
    ($($name:ident)+; $last:ident) => {
        #[allow(non_snake_case)]
        impl<$($name: RadixKey,)+ $last: RadixKey> RadixKey for ($($name,)+ $last) {
            const BYTES: usize = $($name::BYTES +)+ $last::BYTES;

            #[inline]
            fn digit(&self, digit: usize) -> u8 {
                // The last element is the least significant one.
                let ($($name,)+ $last) = self;
                let mut digit = digit;
                if digit < $last::BYTES {
                    return $last.digit(digit);
                }
                digit -= $last::BYTES;
                radix_key_tuple!(@digit digit; $($name)+)
            }

            #[inline]
            fn write(&self, out: &mut Vec<u8>) {
                let ($($name,)+ $last) = self;
                $($name.write(out);)+
                $last.write(out);
            }

            #[inline]
            fn read(bytes: &[u8]) -> Option<Self> {
                let rest = bytes;
                $(
                    let (head, rest) = rest.split_at_checked($name::BYTES)?;
                    let $name = $name::read(head)?;
                )+
                Some(($($name,)+ $last::read(rest)?))
            }
        }
    };
    // digits of the leading elements, most significant first: walk them from the last one
    (@digit $digit:ident; $first:ident) => { $first.digit($digit) };
    (@digit $digit:ident; $first:ident $second:ident) => {
        if $digit < $second::BYTES { $second.digit($digit) } else { $first.digit($digit - $second::BYTES) }
    };
}

radix_key_tuple!(A; B);
radix_key_tuple!(A B; C);

#[cfg(test)]
mod test {
    use rand::Rng;

    use super::RadixKey;

    /// Checks that comparing digits agrees with `Ord`, and that keys survive encoding.
    fn check<K: RadixKey + std::fmt::Debug>(keys: &[K]) {
        let by_digits = |k: &K| (0..K::BYTES).rev().map(|d| k.digit(d)).collect::<Vec<u8>>();
        for pair in keys.windows(2) {
            assert_eq!(
                by_digits(&pair[0]).cmp(&by_digits(&pair[1])),
                pair[0].cmp(&pair[1]),
                "{pair:?}"
            );
        }
        for key in keys {
            let mut encoded = Vec::new();
            key.write(&mut encoded);
            assert_eq!(encoded.len(), K::BYTES);
            assert_eq!(K::read(&encoded), Some(*key));
            assert_eq!(K::read(&encoded[1..]), None);
        }
    }

    #[test]
    fn test_digit_order_matches_ord() {
        let mut rng = rand::thread_rng();
        let n = 1000;
        check(&(0..n).map(|_| rng.gen::<u128>()).collect::<Vec<_>>());
        check(&(0..n).map(|_| rng.gen::<u16>()).collect::<Vec<_>>());
        check(&(0..n).map(|_| rng.gen::<i64>()).collect::<Vec<_>>());
        check(&(0..n).map(|_| rng.gen::<i8>()).collect::<Vec<_>>());
        check(&[i32::MIN, -1, 0, 1, i32::MAX]);
        check(&(0..n).map(|_| rng.gen::<[u8; 3]>()).collect::<Vec<_>>());
        check(&(0..n).map(|_| rng.gen::<char>()).collect::<Vec<_>>());
        check(&[false, true]);
        check(
            &(0..n)
                .map(|_| (rng.gen_range(-2i32..2), rng.gen::<bool>()))
                .collect::<Vec<_>>(),
        );
        let triple = |_| (rng.gen_range(0u8..3), rng.gen_range(-3i16..3), rng.gen_range(0u64..3));
        check(&(0..n).map(triple).collect::<Vec<_>>());
    }
}
