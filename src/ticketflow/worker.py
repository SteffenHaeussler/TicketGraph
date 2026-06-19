"""Worker entrypoint for the durable LangGraph/Postgres runner (Milestone 4.2).

The runner loop lives in :mod:`ticketflow.runner`; this module stays the process
entrypoint that the ``worker`` make target invokes. The target is renamed to
``runner`` in Milestone 7.6.
"""

import asyncio

from ticketflow.runner import main

__all__ = ["main"]


if __name__ == "__main__":
    asyncio.run(main())
