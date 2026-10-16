from concurrent.futures import ThreadPoolExecutor


def test_create_tables_survives_concurrent_runs(run_ddl):
    # Краулер и опрос стартуют в одну минуту,каждый начинает с create_tables
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(run_ddl) for _ in range(18)]
    for future in futures:
        future.result()
