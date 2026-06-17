from scripts import reset


async def test_run_reset_clears_read_model_and_reports_counts(monkeypatch):
    calls: list[str | None] = []

    def fake_clear(*, database_url: str | None = None) -> int:
        calls.append(database_url)
        return 3

    monkeypatch.setattr(reset.readmodel, "clear", fake_clear)

    summary = await reset.run_reset(database_url="postgresql://example/tickets")

    assert summary == {"read_model_rows_cleared": 3}
    assert calls == ["postgresql://example/tickets"]
