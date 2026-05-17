"""Build the MCP server ASGI app to mount at ``/mcp``.

Uses the official ``mcp`` SDK's low-level ``Server`` because the
high-level ``FastMCP`` decorators don't (currently) expose the
Authorization header to tool bodies — we need that to identify the
caller for metering. With low-level, the tool dispatch goes through
``call_tool`` which is wrapped by our ``BearerAuthMiddleware`` on the
mounted Starlette app, so the ContextVar is always populated.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from starlette.applications import Starlette
from starlette.routing import Mount

from ocoi_api.mcp.auth import BearerAuthMiddleware
from ocoi_api.mcp.metering import QuotaExceeded
from ocoi_api.mcp.tools import TOOL_DEFS

_log = logging.getLogger("ocoi.mcp.server")


def build_mcp_app() -> Starlette:
    """Return the ASGI app that ``main.py`` mounts at ``/mcp``.

    Imports of the MCP SDK happen lazily inside this function so the
    project still imports cleanly if the dependency is missing (kill
    switch + dev environments without the package installed)."""
    try:
        import mcp.types as mtypes
        from mcp.server import Server as MCPServer
        from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    except Exception as exc:  # noqa: BLE001
        _log.warning("MCP SDK not importable (%s); /mcp will 404", exc)
        return _disabled_app()

    server: MCPServer = MCPServer("ocoi")

    # ── list_tools ──
    @server.list_tools()  # type: ignore[misc]
    async def _list_tools() -> list[mtypes.Tool]:
        return [
            mtypes.Tool(
                name=t["name"],
                description=t["description"],
                inputSchema=t["schema"],
            )
            for t in TOOL_DEFS
        ]

    # ── call_tool ──
    tools_by_name = {t["name"]: t for t in TOOL_DEFS}

    @server.call_tool()  # type: ignore[misc]
    async def _call_tool(name: str, arguments: dict[str, Any]) -> list[mtypes.TextContent]:
        tool = tools_by_name.get(name)
        if tool is None:
            return [mtypes.TextContent(type="text", text=f"Unknown tool: {name}")]
        try:
            result = await tool["fn"](**(arguments or {}))
        except QuotaExceeded as q:
            return [mtypes.TextContent(
                type="text",
                text=json.dumps({"error": "quota_exceeded", "message": str(q)}),
            )]
        except Exception as e:
            _log.exception("MCP tool %s failed", name)
            return [mtypes.TextContent(
                type="text",
                text=json.dumps({"error": type(e).__name__, "message": str(e)}),
            )]
        return [mtypes.TextContent(
            type="text",
            text=json.dumps(result, ensure_ascii=False, default=str),
        )]

    # ── Streamable HTTP transport ──
    session_manager = StreamableHTTPSessionManager(
        app=server,
        event_store=None,
        json_response=True,  # one-shot response — simpler for short tool calls
        stateless=True,      # no server-side session affinity
    )

    async def handle_mcp(scope, receive, send) -> None:
        await session_manager.handle_request(scope, receive, send)

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app):
        async with session_manager.run():
            yield

    inner = Starlette(
        debug=False,
        routes=[Mount("/", app=handle_mcp)],
        lifespan=lifespan,
    )
    inner.add_middleware(BearerAuthMiddleware)
    return inner


def _disabled_app() -> Starlette:
    from starlette.responses import JSONResponse

    async def disabled(_request):
        return JSONResponse(
            {"error": "mcp_disabled", "message": "MCP server is not available"},
            status_code=503,
        )

    return Starlette(routes=[Mount("/", app=disabled)])
