from mcp.server.fastmcp import FastMCP
from pyiceberg.catalog import load_catalog

catalog_path = "/Users/diptimanraichaudhuri/testing_space/iceberg_playground/"
catalog = load_catalog("pyarrowtest",
                    **{
                        "uri": f"sqlite:///{catalog_path}/pyiceberg_catalog/pyarrow_catalog.db"
                    })

mcp = FastMCP("PyIceberg MCP stdio")

@mcp.tool()
def describe_catalog_properties():
    """Retrieve Iceberg catalog properties from the sqlite catalog.
       Prints catalog properties
    """
    return {
        "catalog_properties": catalog.properties
    }
    
@mcp.tool()
def list_namespaces():
    """Retrieve namespaces from Iceberg catalog.
       Print namesapces
    """
    return {
        "namespaces": catalog.list_namespaces()
    }

@mcp.tool()
def list_tables(namesapce):
    """List tables from the namesapce
       Prints tables
    """
    return {
        "tables": catalog.list_tables(namesapce)
    }

@mcp.tool()
def detect_partitions(table):
    """Check the partition of the table
       Print the name of the partition and how may partitions are there for the table
    """
    part_table_ = catalog.load_table(table)
    return {
        "partitions": part_table_.inspect.partitions()
    }
    
@mcp.tool()
def query(table):
    """Query the Iceberg table
       Print the result in tabular form
    """
    q_table = catalog.load_table(table)
    return {
        "result": q_table.scan().to_pandas()
    }
    
if __name__ =="__main__":
    print("Starting PyIceberg MCP Server in stdio transport mode....")
    mcp.run(transport="stdio")
