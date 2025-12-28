pub fn setup(workers: usize) {
    if workers > 0 {
        // Setup rayon global pool
        let _ = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build_global();
    }
}