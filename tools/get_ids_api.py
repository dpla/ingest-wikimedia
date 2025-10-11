import click

from ingest_wikimedia.tools_context import ToolsContext


@click.command()
@click.argument("partner")
@click.option("--no-shard", is_flag=True)
@click.option("--add-query")
def main(partner: str, no_shard: bool, add_query: str):
    tools_context = ToolsContext.init(partner)
    dpla = tools_context.get_dpla()
    dpla.check_partner(partner)
    dpla.get_ids(partner, add_query, no_shard)


if __name__ == "__main__":
    main()
