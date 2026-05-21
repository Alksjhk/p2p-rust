use crate::crypto::Crypto;
use anyhow::Result;

pub struct NoopCrypto;

impl Crypto for NoopCrypto {
    fn encrypt(&self, _buf: &mut [u8]) -> Result<()> { Ok(()) }
    fn decrypt(&self, _buf: &mut [u8]) -> Result<()> { Ok(()) }
    fn max_overhead(&self) -> usize { 0 }
}
