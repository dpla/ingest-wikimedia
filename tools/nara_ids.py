from typing import IO

import click

from ingest_wikimedia.tools_context import ToolsContext


@click.command()
@click.argument("output", type=click.File("w"))
def main(output: IO):
    tools_context = ToolsContext.init("nara")
    dpla = tools_context.get_dpla()
    for nara_id in dpla.get_nara_ids():
        output.write(nara_id)
        output.write("\n")


if __name__ == "__main__":
    main()
