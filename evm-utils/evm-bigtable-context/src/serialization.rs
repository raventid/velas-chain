pub trait FixedSizedKey: Ord {
    const SIZE: usize;
    // TODO: replace by writter fn write_ord_bytes(&self, writer: impl Write) -> Result<usize>;
    fn write_ord_bytes(&self, buffer: &mut [u8]); // write failure is assertion

    // THIS trait is firstly implemented for abstraction over bigtable, thats why in tuple this method will reverse last element byte representation.
    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self;

    fn hex_encoded_reverse(&self) -> String {
        let mut buffer = vec![0u8; Self::SIZE];
        self.write_ord_bytes(&mut buffer);
        hex::encode(&buffer)
    }
}

// TODO: Sealed
// TODO: Implement for all tuples.
pub trait MultiPrefixKey: FixedSizedKey {
    /// Type of key without last component
    type Prefixes;
    type Suffix;
    fn rebuild(head: Self::Prefixes, tail: Self::Suffix) -> Self
    where
        Self: Sized;
}

impl<T1: FixedSizedKey, T2: FixedSizedKey> FixedSizedKey for (T1, T2) {
    const SIZE: usize = T1::SIZE + T2::SIZE;
    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        let (b1, b2) = buffer.split_at_mut(T1::SIZE);
        self.0.write_ord_bytes(b1);
        self.1.write_ord_bytes(b2);
        // reverse suffix
        for i in b2 {
            *i ^= 0xff;
        }
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let (b1, preb2) = buffer.split_at(T1::SIZE);
        let mut b2 = vec![0u8; T2::SIZE];
        for (r, i) in b2.iter_mut().zip(preb2) {
            *r = i ^ 0xff;
        }
        (
            T1::from_buffer_ord_bytes(b1),
            T2::from_buffer_ord_bytes(&b2),
        )
    }
}

impl<T1: FixedSizedKey, T2: FixedSizedKey, T3: FixedSizedKey> FixedSizedKey for (T1, T2, T3) {
    const SIZE: usize = T1::SIZE + T2::SIZE + T3::SIZE;
    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        let (b1, b2) = buffer.split_at_mut(T1::SIZE);
        self.0.write_ord_bytes(b1);
        let (b2, b3) = b2.split_at_mut(T2::SIZE);
        self.1.write_ord_bytes(b2);
        self.2.write_ord_bytes(b3);

        // reverse suffix
        for i in b3 {
            *i ^= 0xff;
        }
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let (b1, b2) = buffer.split_at(T1::SIZE);

        let (b2, preb3) = b2.split_at(T2::SIZE);
        let mut b3 = vec![0u8; T3::SIZE];
        for (r, i) in b3.iter_mut().zip(preb3) {
            *r = *i ^ 0xff;
        }
        (
            T1::from_buffer_ord_bytes(b1),
            T2::from_buffer_ord_bytes(&b2),
            T3::from_buffer_ord_bytes(&b3),
        )
    }
}

impl<T1: FixedSizedKey, T2: FixedSizedKey> MultiPrefixKey for (T1, T2) {
    type Prefixes = (T1,);
    type Suffix = T2;
    fn rebuild((head,): Self::Prefixes, tail: Self::Suffix) -> Self
    where
        Self: Sized,
    {
        (head, tail)
    }
}

impl<T1: FixedSizedKey, T2: FixedSizedKey, T3: FixedSizedKey> MultiPrefixKey for (T1, T2, T3) {
    type Prefixes = (T1, T2);
    type Suffix = T3;
    fn rebuild((head, head2): Self::Prefixes, tail: Self::Suffix) -> Self
    where
        Self: Sized,
    {
        (head, head2, tail)
    }
}

/// Is default = min value?;
pub trait RangeValue {
    fn min() -> Self;
    fn max() -> Self;
}
impl RangeValue for u32 {
    fn min() -> Self {
        Self::MIN
    }
    fn max() -> Self {
        Self::MAX
    }
}

impl<T1: RangeValue, T2: RangeValue> RangeValue for (T1, T2) {
    fn min() -> Self {
        (T1::min(), T2::min())
    }
    fn max() -> Self {
        (T1::max(), T2::max())
    }
}
impl<T1: RangeValue, T2: RangeValue, T3: RangeValue> RangeValue for (T1, T2, T3) {
    fn min() -> Self {
        (T1::min(), T2::min(), T3::min())
    }
    fn max() -> Self {
        (T1::max(), T2::max(), T3::max())
    }
}

// TODO: macros
impl FixedSizedKey for u32 {
    const SIZE: usize = std::mem::size_of::<Self>();
    fn write_ord_bytes(&self, buffer: &mut [u8]) {
        buffer.copy_from_slice(&self.to_be_bytes())
    }

    fn from_buffer_ord_bytes(buffer: &[u8]) -> Self {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(buffer);
        u32::from_be_bytes(bytes)
    }
}
