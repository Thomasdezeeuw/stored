use stored::key::{key, InvalidKeyStr, Key, KeyCalculator};

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
    let blob = b"Hello world";
    let mut calc = KeyCalculator::new();
    calc.update(&blob[..6]);
    calc.update(&blob[6..]);
    assert_eq!(calc.finish(), Key::for_blob(blob));
}

#[test]
fn key_macro() {
    let key = key!("b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47");
    let expected = "b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47";
    assert_eq!(format!("{key}"), expected);
}
