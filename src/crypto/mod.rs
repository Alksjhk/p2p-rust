use anyhow::Result;

pub trait Crypto: Send + Sync {
    fn encrypt(&self, buf: &mut [u8]) -> Result<()>;
    fn decrypt(&self, buf: &mut [u8]) -> Result<()>;
    fn max_overhead(&self) -> usize;
}

pub mod noop;
