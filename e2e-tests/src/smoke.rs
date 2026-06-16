use heph_rt::Access;

use crate::start_process;

const BLOB: &[u8] = b"Hello, World!";

pub async fn run<RT: Access>(rt: RT) {
    let (mut client, _stored) = start_process(&rt).await;

    let key = client.add(BLOB.into()).await.unwrap();

    let got_blob = client.get(&key).await.unwrap();
    assert_eq!(got_blob.as_deref(), Some(BLOB));

    let got_contains = client.contains(&key).await.unwrap();
    assert!(got_contains);

    let got_blobs_stored = client.blobs_stored().await.unwrap();
    assert_eq!(got_blobs_stored, 1);

    client.remove(&key).await.unwrap();

    let got_blob = client.get(&key).await.unwrap();
    assert!(got_blob.is_none());

    let got_contains = client.contains(&key).await.unwrap();
    assert!(!got_contains);

    let got_blobs_stored = client.blobs_stored().await.unwrap();
    assert_eq!(got_blobs_stored, 0);
}
