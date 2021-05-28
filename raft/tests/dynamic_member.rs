mod test_raft;

use std::time::Duration;

use test_raft::{Action, TestHarness};
use xraft::Role;

#[tokio::test]
async fn dynamic_member() {
    let test = TestHarness::default();
    test.add_node(1).await;
    test.initialize().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(test.metrics(1).await.unwrap().leader, Some(1));
    assert_eq!(test.metrics(1).await.unwrap().role, Role::Leader);

    test.write(Action::put("a", 1)).await.unwrap();
    test.write(Action::put("b", 2)).await.unwrap();

    test.add_node(2).await;
    test.add_non_voter(2).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(test.read_from(1, "a").await.unwrap(), Some(1));
    assert_eq!(test.read_from(1, "b").await.unwrap(), Some(2));

    assert_eq!(test.read_from(2, "a").await.unwrap(), Some(1));
    assert_eq!(test.read_from(2, "b").await.unwrap(), Some(2));
}
