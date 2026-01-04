#[inline(always)]

pub fn cmp_le_bytes(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let max_len = a.len().max(b.len());

    for i in (0..max_len).rev() {
        let av = a.get(i).copied().unwrap_or(0);
        let bv = b.get(i).copied().unwrap_or(0);
        if av != bv {
            return av.cmp(&bv);
        }
    }
    std::cmp::Ordering::Equal
}
