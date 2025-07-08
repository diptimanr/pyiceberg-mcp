from mcp import ClientSession
from typing import Optional
from contextlib import AsyncExitStack
from mcp.client.streamable_http import streamablehttp_client
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain_anthropic import ChatAnthropic
import asyncio

from dotenv import load_dotenv

load_dotenv()

model = ChatAnthropic(
    model="claude-sonnet-4-20250514",
    temperature=0,
    max_tokens=1024,
    timeout=None,
    max_retries=2
)

class MCPClient:
    """MCP Client for interacting with an MCP Streamable HTTP server
    """
    def __init__(self):
        # Initialize session and client objects
        self.session: Optional[ClientSession] = None
        self.exit_stack = AsyncExitStack()

    async def connect_to_streamable_http_server(
        self, server_url: str, headers: Optional[dict] = None
    ):
        """Connect to an MCP server running with HTTP Streamable transport
        """
        self._streams_context = streamablehttp_client(
            url=server_url,
            headers=headers or {},
        )
        read_stream, write_stream, _ = await self._streams_context.__aenter__()  # pylint: disable=E1101

        self._session_context = ClientSession(read_stream, write_stream)  # pylint: disable=W0201
        self.session: ClientSession = await self._session_context.__aenter__()  # pylint: disable=C2801

        await self.session.initialize()
        print("MCP Session Initialized with transport mode Streamable HTTP ...")

    async def cleanup(self):
        """Properly clean up the session and streams
        """
        if self._session_context:
            await self._session_context.__aexit__(None, None, None)
        if self._streams_context:
            await self._streams_context.__aexit__(None, None, None)  # pylint: disable=E1101
async def main():
    """Main function to run the MCP client
    """
    client = MCPClient()

    try:
        await client.connect_to_streamable_http_server(
            f"http://localhost:31267/mcp"
        )
        # List available tools
        tools = await load_mcp_tools(client.session)
        print(f"Loaded Tools: {[tool.name for tool in tools]}")

        agent = create_react_agent(model, tools)
        print("ReAct Agent Created.")

        print(f"Invoking agent with query")
        response = await agent.ainvoke({
            "messages": [("user",
                          "list namespaces and tables from Iceberg catalog.\n"
                          "Then query the table and return the device_make of the device that consumed the highest charge.")]
        })
        return response["messages"][-1].content
    finally:
        await client.cleanup()

if __name__ == "__main__":
    result = asyncio.run(main())
    print("\nAgent Final Response:")
    print(result)
