use crate::peer::{COORDINATOR_MAGIC, MAGIC_LENGTH, PARTICIPANT_MAGIC};

#[test]
fn magic_bytes() {
    assert_eq!(MAGIC_LENGTH, COORDINATOR_MAGIC.len());
    assert_eq!(MAGIC_LENGTH, PARTICIPANT_MAGIC.len());
}
