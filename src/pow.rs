use argon2::{Algorithm, Argon2, Params, Version};

pub const POW_MEMORY_KIB: u32 = 2 * 1024 * 1024;
pub const POW_ITERATIONS: u32 = 1;
pub const POW_PARALLELISM: u32 = 1;

pub trait PowHasher: Send + Sync + 'static {
    fn hash(&self, header_base: &[u8], nonce: u64) -> [u8; 32];
}

#[derive(Debug, Clone, Copy)]
pub struct Argon2PowHasher {
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

impl Default for Argon2PowHasher {
    fn default() -> Self {
        Self {
            memory_kib: POW_MEMORY_KIB,
            iterations: POW_ITERATIONS,
            parallelism: POW_PARALLELISM,
        }
    }
}

impl PowHasher for Argon2PowHasher {
    fn hash(&self, header_base: &[u8], nonce: u64) -> [u8; 32] {
        let params = match Params::new(self.memory_kib, self.iterations, self.parallelism, Some(32))
        {
            Ok(params) => params,
            Err(_) => return [0u8; 32],
        };
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let mut out = [0u8; 32];
        if argon2
            .hash_password_into(&nonce.to_le_bytes(), header_base, &mut out)
            .is_err()
        {
            return [0u8; 32];
        }
        out
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeterministicTestHasher;

impl PowHasher for DeterministicTestHasher {
    fn hash(&self, header_base: &[u8], nonce: u64) -> [u8; 32] {
        // Cheap deterministic hash for tests: nonce-seeded rolling mixer over header bytes.
        let mut s0 = nonce.wrapping_mul(0x9E3779B97F4A7C15);
        let mut s1 = (!nonce).wrapping_mul(0xBF58476D1CE4E5B9);
        for &b in header_base {
            s0 ^= (b as u64).wrapping_mul(0x100000001B3);
            s0 = s0.rotate_left(13).wrapping_add(s1);
            s1 ^= s0.rotate_right(7);
        }

        let mut out = [0u8; 32];
        for i in 0..4 {
            s0 ^= s1.rotate_left(17);
            s1 ^= s0.rotate_right(11);
            let v = s0
                .wrapping_add(s1)
                .wrapping_add((i as u64).wrapping_mul(0x9E3779B97F4A7C15));
            out[i * 8..(i + 1) * 8].copy_from_slice(&v.to_be_bytes());
        }
        out
    }
}

pub fn check_target(hash: [u8; 32], target: [u8; 32]) -> bool {
    for i in 0..32 {
        if hash[i] < target[i] {
            return true;
        }
        if hash[i] > target[i] {
            return false;
        }
    }
    true
}

pub fn difficulty_to_target(difficulty: u64) -> [u8; 32] {
    if difficulty == 0 {
        return [0u8; 32];
    }

    let numerator = [u64::MAX; 4];
    let mut quotient = [0u64; 4];
    let mut rem = 0u64;

    for (i, limb) in numerator.into_iter().enumerate() {
        let high = rem as u128;
        let low = limb as u128;
        let dividend = (high << 64) | low;
        quotient[i] = (dividend / difficulty as u128) as u64;
        rem = (dividend % difficulty as u128) as u64;
    }

    let mut target = [0u8; 32];
    for (i, word) in quotient.into_iter().enumerate() {
        target[i * 8..(i + 1) * 8].copy_from_slice(&word.to_be_bytes());
    }
    target
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_target_orders_big_endian() {
        let mut hash = [0x10u8; 32];
        let mut target = [0x20u8; 32];
        assert!(check_target(hash, target));

        hash[0] = 0x30;
        assert!(!check_target(hash, target));

        target = hash;
        assert!(check_target(hash, target));
    }

    #[test]
    fn difficulty_zero_is_zero_target() {
        assert_eq!(difficulty_to_target(0), [0u8; 32]);
    }

    #[test]
    fn higher_difficulty_means_lower_target() {
        let t1 = difficulty_to_target(1);
        let t2 = difficulty_to_target(2);
        let t4 = difficulty_to_target(4);

        assert!(t1 > t2);
        assert!(t2 > t4);
    }

    #[test]
    fn deterministic_hasher_is_stable() {
        let hasher = DeterministicTestHasher;
        let h1 = hasher.hash(&[1, 2, 3, 4], 42);
        let h2 = hasher.hash(&[1, 2, 3, 4], 42);
        let h3 = hasher.hash(&[1, 2, 3, 4], 43);
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }
}