use digest::{Digest, DynDigest};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Sha512};

use crate::{Error, Result};

// https://github.com/opencontainers/image-spec/blob/main/descriptor.md#digests
pub struct OciDigest {
    algorithm: RegisteredImageSpecAlgorithm,
    encoded: String,
}

impl TryFrom<&str> for OciDigest {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        let i = match s.find(':') {
            Some(i) => i,
            None => return Err(Error::InvalidDigest(s.to_string())),
        };
        let algo: &str = match s.get(..i) {
            Some(a) => a,
            None => return Err(Error::InvalidDigest(s.to_string())),
        };
        let encoded: &str = match s.get(i..) {
            Some(e) => e,
            None => return Err(Error::InvalidDigest(s.to_string())),
        };
        let algorithm = match algo {
            "sha256" => RegisteredImageSpecAlgorithm::Sha256,
            "sha512" => RegisteredImageSpecAlgorithm::Sha512,
            _ => {
                return Err(Error::InvalidDigest(s.to_string()));
            }
        };

        Ok(Self {
            algorithm,
            encoded: encoded.to_string(),
        })
    }
}

impl From<OciDigest> for String {
    fn from(d: OciDigest) -> String {
        format!("{}:{}", String::from(&d.algorithm), d.encoded)
    }
}

impl From<&OciDigest> for String {
    fn from(d: &OciDigest) -> String {
        format!("{}:{}", String::from(&d.algorithm), d.encoded)
    }
}

impl OciDigest {
    pub fn digester(&self) -> Digester {
        Digester {
            digester: match self.algorithm {
                RegisteredImageSpecAlgorithm::Sha256 => Box::new(Sha256::new()),
                RegisteredImageSpecAlgorithm::Sha512 => Box::new(Sha512::new()),
            },
        }
    }
}

enum RegisteredImageSpecAlgorithm {
    Sha256,
    Sha512,
}

impl From<&RegisteredImageSpecAlgorithm> for String {
    fn from(a: &RegisteredImageSpecAlgorithm) -> String {
        match a {
            RegisteredImageSpecAlgorithm::Sha512 => String::from("sha512"),
            RegisteredImageSpecAlgorithm::Sha256 => String::from("sha256"),
        }
    }
}

pub struct Digester {
    digester: Box<dyn DynDigest + Send + Sync>,
}

impl Digester {
    pub fn update(&mut self, data: &[u8]) {
        self.digester.update(data);
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct DigestState {
    sha256_state: Box<[u8]>,
    sha512_state: Box<[u8]>,
}
