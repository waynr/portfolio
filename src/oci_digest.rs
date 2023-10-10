use digest::{Digest, DynDigest};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};
use crate::sha256::{State256, Sha256};
use crate::sha512::{State512, Sha512};

// https://github.com/opencontainers/image-spec/blob/main/descriptor.md#digests
#[derive(Clone, Debug)]
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
        let encoded: &str = match s.get(i + 1..) {
            Some(e) if e.len() > 0 => e,
            Some(_) => return Err(Error::InvalidDigest(s.to_string())),
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

impl TryFrom<&[u8]> for OciDigest {
    type Error = Error;
    fn try_from(bs: &[u8]) -> Result<Self> {
        let mut hasher = Sha256::new();
        Digest::update(&mut hasher, bs);
        let s = hasher.finalize();

        Ok(Self {
            algorithm: RegisteredImageSpecAlgorithm::Sha256,
            encoded: format!("{:x}", s),
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

#[derive(Clone, Debug)]
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

#[cfg(test)]
mod test {
    use rstest::*;

    use super::*;

    #[rstest]
    #[case::meow("sha256:meow", Ok(OciDigest {
        algorithm: RegisteredImageSpecAlgorithm::Sha256,
        encoded: String::from("meow"),
    }))]
    #[case::meow("sha512:meow", Ok(OciDigest {
        algorithm: RegisteredImageSpecAlgorithm::Sha512,
        encoded: String::from("meow"),
    }))]
    #[case::meow("sha666:meow", Err(Error::InvalidDigest(String::from("sha666:meow"))))]
    #[case::meow("sha256meow", Err(Error::InvalidDigest(String::from("sha256meow"))))]
    #[case::meow("sha256:", Err(Error::InvalidDigest(String::from("sha256:"))))]
    #[case::meow(":meow", Err(Error::InvalidDigest(String::from(":meow"))))]
    fn validate_try_from(#[case] input: &str, #[case] expected: Result<OciDigest>) {
        let actual: Result<OciDigest> = input.try_into();
        match (expected, actual) {
            (Ok(exp), Ok(act)) => {
                assert_eq!(exp, act);
            }
            (Ok(s), Err(e)) => {
                assert!(false, "expected Ok( {s:?} ) got Err( {e:?} )");
            }
            (Err(exp), Err(act)) => {
                let exp = format!("{exp}");
                let act = format!("{act}");
                assert_eq!(exp, act);
            }
            (Err(e), Ok(s)) => {
                assert!(false, "expected Err( {e:?} ) got Ok( {s:?} )");
            }
        }
    }
}
