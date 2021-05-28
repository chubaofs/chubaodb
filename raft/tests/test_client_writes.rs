mod test_raft;

use std::time::Duration;

use futures_util::StreamExt;
use test_raft::TestHarness;

use crate::test_raft::Action;

#[tokio::test]
async fn client_writes() {
    let test = TestHarness::default();
    test.add_node(1).await;
    test.add_node(2).await;
    test.add_node(3).await;
    test.initialize().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;

    futures_util::stream::iter(0..4)
        .for_each_concurrent(None, {
            let test = test.clone();
            move |t| {
                let test = test.clone();
                async move {
                    for i in 0..1000 {
                        test.write(Action::put(format!("{}/{}", t, i), i))
                            .await
                            .unwrap();
                    }
                }
            }
        })
        .await;

    for t in 0..4 {
        for i in 0..1000 {
            assert_eq!(test.read(format!("{}/{}", t, i)).await.unwrap(), Some(i));
        }
    }

    for n in 1..=3 {
        for t in 0..4 {
            for i in 0..1000 {
                assert_eq!(
                    test.read_from(n, format!("{}/{}", t, i)).await.unwrap(),
                    Some(i)
                );
            }
        }
    }
}
