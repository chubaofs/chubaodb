use faiss::{index_factory, Idx, Index, MetricType};

fn main() {
    let mut index = index_factory(8, "Flat", MetricType::L2).unwrap();
    let some_data = &[
        7.5_f32, -7.5, 7.5, -7.5, 7.5, 7.5, 7.5, 7.5, -1., 1., 1., 1., 1., 1., 1., -1., 0., 0., 0.,
        1., 1., 0., 0., -1., 100., 100., 100., 100., -100., 100., 100., 100., 120., 100., 100.,
        105., -100., 100., 100., 105.,
    ];
    index.add(some_data).unwrap();
    assert_eq!(index.ntotal(), 5);

    let my_query = [0.; 8];

    let result = index.search(&my_query, 5).unwrap();
    assert_eq!(
        result.labels,
        vec![
            Idx::new(2),
            Idx::new(1),
            Idx::new(0),
            Idx::new(3),
            Idx::new(4)
        ]
    );
    assert!(result.distances.iter().all(|x| *x > 0.));

    let my_query = [100.; 8];
    let result = index.search(&my_query, 5).unwrap();
    assert_eq!(
        result.labels,
        vec![
            Idx::new(3),
            Idx::new(4),
            Idx::new(0),
            Idx::new(1),
            Idx::new(2)
        ]
    );
    assert!(result.distances.iter().all(|x| *x > 0.));

    let my_query = vec![
        0., 0., 0., 0., 0., 0., 0., 0., 100., 100., 100., 100., 100., 100., 100., 100.,
    ];
    let result = index.search(&my_query, 5).unwrap();
    assert_eq!(
        result.labels,
        vec![
            Idx::new(2),
            Idx::new(1),
            Idx::new(0),
            Idx::new(3),
            Idx::new(4),
            Idx::new(3),
            Idx::new(4),
            Idx::new(0),
            Idx::new(1),
            Idx::new(2)
        ]
    );
}
