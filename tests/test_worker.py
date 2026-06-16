from ticketflow import llm_worker, worker


def test_worker_exposes_async_main():
    assert callable(worker.main)


def test_llm_worker_exposes_async_main():
    assert callable(llm_worker.main)
