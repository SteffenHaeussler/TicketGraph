from scripts import reset
from ticketflow import readmodel
from ticketflow.models import TicketResult, TicketStatus


async def test_run_reset_clears_read_model_and_reports_counts(tmp_path):
    db_path = str(tmp_path / "read.db")
    readmodel.save_result(
        TicketResult(
            ticket_id="old",
            status=TicketStatus.RESOLVED,
            reply_text="archived",
        ),
        db_path,
    )

    summary = await reset.run_reset(db_path=db_path)

    assert summary == {"read_model_rows_cleared": 1}
    assert readmodel.load_result("old", db_path) is None
