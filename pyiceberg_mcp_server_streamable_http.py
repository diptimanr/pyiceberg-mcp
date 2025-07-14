from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langchain_anthropic import ChatAnthropic
import asyncio

from fastmcp.client.transports import StreamableHttpTransport
from fastmcp import Client

from dotenv import load_dotenv

load_dotenv()

model = ChatAnthropic(
    model="claude-sonnet-4-20250514",
    temperature=0,
    max_tokens=1024,
    timeout=None,
    max_retries=2
)

async def main():
        try:
            transport = StreamableHttpTransport(url="http://localhost:31267/mcp")
            async with Client(transport=transport) as client:
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
            client.close()

if __name__ == "__main__":
    result = asyncio.run(main())
    print("\nAgent Final Response:")
    print(result)
