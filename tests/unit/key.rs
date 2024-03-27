use std::hash::{Hash, Hasher};
use std::io::{self, Read};

use stored::key::{key, InvalidKeyStr, Key, KeyHasher};

#[test]
fn to_owned() {
    let bytes: Vec<u8> = (0..64).collect();
    let key1 = Key::from_bytes(&bytes);
    assert_eq!(key1.as_bytes(), &*bytes);
    let key2 = key1.to_owned();
    assert_eq!(key1, &key2);
}

#[test]
fn formatting() {
    let key = Key::for_blob(b"Hello world");
    let expected = "b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    assert_eq!(format!("{key}"), expected); // `fmt::Display` trait.
    assert_eq!(format!("{:?}", key), expected); // `fmt::Debug` trait.
    assert_eq!(key.to_string(), expected); // ToString trait.
}

#[test]
fn parsing() {
    let got = Key::for_blob(b"Hello world");
    let expected = key!("b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47");
    assert_eq!(got, expected);
}

#[test]
fn parsing_errors() {
    // Invalid input length.
    let input = "";
    assert_eq!(input.parse::<Key>(), Err(InvalidKeyStr));

    // Invalid hex digits.
    let input = "G7f783baed8297f0db917462184ff4f08e69c2d5e\
                     5f79a942600f9725f58ce1f29c18139bf80b06c0f\
                     ff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    assert_eq!(input.parse::<Key>(), Err(InvalidKeyStr));
}

#[test]
fn key_calculator() {
    let want = Key::for_blob(b"Hello world");
    let reader = io::Cursor::new(b"Hello world");
    let mut calc = Key::calculator(reader);
    let mut buf = [0; 6];
    assert_eq!(calc.read(&mut buf).unwrap(), 6);
    assert_eq!(&buf, b"Hello ");
    assert_eq!(calc.read(&mut buf).unwrap(), 5);
    assert_eq!(&buf, b"world "); // Last space from previous read.
    assert_eq!(calc.finish(), want);
}

#[test]
fn key_calculator_skip_n() {
    let want = Key::for_blob(b"Hello world");
    let reader = io::Cursor::new(b"123Hello world");
    let mut calc = Key::calculator_skip(reader, 3);
    let mut buf = [0; 7];
    assert_eq!(calc.read(&mut buf).unwrap(), 7);
    assert_eq!(&buf, b"123Hell");
    assert_eq!(calc.read(&mut buf).unwrap(), 7);
    assert_eq!(&buf, b"o world");
    assert_eq!(calc.finish(), want);
}

#[test]
fn key_hashing() {
    let keys = [
        Key::for_blob(b"Hello world"),
        Key::for_blob(b"Hello world1"),
        Key::for_blob(b"Hello world2"),
        Key::for_blob(b"Hello world3"),
    ];

    for keys in keys.windows(2) {
        let result1 = hash_key(&keys[0]);
        let result2 = hash_key(&keys[1]);
        let result2b = hash_key(&keys[1]);

        assert_ne!(result1, result2);
        assert_eq!(result2, result2b);
    }

    fn hash_key(key: &Key) -> u64 {
        let mut hasher = KeyHasher::default();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[test]
fn key_macro() {
    let key = key!("b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47");
    let expected = "b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    assert_eq!(format!("{key}"), expected);
}
