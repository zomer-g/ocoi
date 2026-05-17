"""MCP server for the OCOI public dataset.

The package exposes:

* ``build_mcp_app()`` — returns the Starlette ASGI app to mount at ``/mcp``.
* ``billing.start_billing_batcher()`` / ``stop_billing_batcher()`` — the
  Stripe usage-record pusher; wired into FastAPI's lifespan in
  ``ocoi_api.main``.
"""

from ocoi_api.mcp.server import build_mcp_app

__all__ = ["build_mcp_app"]
