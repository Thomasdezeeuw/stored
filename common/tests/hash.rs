use coeus_common::Hash;

#[test]
fn hash_to_owned() {
    let bytes: Vec<u8> = (0..64).collect();
    let hash1 = Hash::from_bytes(&bytes);
    let hash2 = hash1.to_owned();
    assert_eq!(hash1, &hash2);
}
