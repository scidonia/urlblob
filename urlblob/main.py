# Copyright 2025 Scidonia Limited
# Licensed under the Apache License, Version 2.0 (the "License");

import sys
import json
import asyncio
from typing import Optional, cast
from httpx import AsyncClient
from urlblob.blob import UrlBlob
from urlblob.common import UrlType

try:
    import typer
    from rich.console import Console
    from rich.table import Table
except ImportError:
    raise RuntimeError("The CLI requires the 'cli' extra: pip install urlblob[cli]")


# Create a state object to hold the URL type
class AppState:
    url_type: Optional[UrlType] = None


state = AppState()

url_type_option = typer.Option(
    None,
    "--url-type",
    "-u",
    help="Override URL type detection (s3, gcp, azure, generic)",
)


def url_type_callback(url_type: Optional[str] = url_type_option) -> Optional[UrlType]:
    """Process the URL type option and store it in the state."""
    if url_type is None:
        return None

    try:
        from urlblob.util import parse_url_type

        state.url_type = parse_url_type(url_type)
        return state.url_type
    except ValueError as e:
        raise typer.BadParameter(str(e))


app = typer.Typer(help="URL Blob - A tool for working with URL data")
app.callback()(url_type_callback)


@app.command()
def put(
    url: str = typer.Argument(..., help="URL to upload to"),
    content: Optional[str] = typer.Argument(
        None, help="Content to upload (omit to read from stdin)"
    ),
    content_type: Optional[str] = typer.Option(
        "text/plain", "--content-type", "-t", help="Content type of the data"
    ),
    lines: bool = typer.Option(
        False, "--lines", "-l", help="Process content as lines of text"
    ),
):
    """Upload content to a URL."""
    if content is None:
        # Read from stdin if no content provided
        content = sys.stdin.read()

    # content is now a string for sure
    content = cast(str, content)

    async def upload():
        async with AsyncClient() as client:
            blob = UrlBlob(url, client, url_type=state.url_type)

            if lines:
                # Split content into lines and use put_lines
                content_lines = content.splitlines()
                await blob.put_lines(lines=content_lines, content_type=content_type)
                typer.echo(f"Uploaded {len(content_lines)} lines to {url}", err=True)
            else:
                # Use regular put
                await blob.put(content=content, content_type=content_type)
                typer.echo(f"Uploaded content to {url}", err=True)

    asyncio.run(upload())


@app.command()
def get(
    url: str = typer.Argument(..., help="URL to download from"),
    range_str: Optional[str] = typer.Argument(
        None,
        help="Byte range in format 'start-end', 'start-', or '-end' (end is inclusive)",
    ),
    start: Optional[int] = typer.Option(None, help="Start byte position"),
    end: Optional[int] = typer.Option(None, help="End byte position"),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Output file (default: stdout)"
    ),
    lines: bool = typer.Option(
        False, "--lines", "-l", help="Process content as lines of text"
    ),
    no_stream: bool = typer.Option(
        False, "--no-stream", help="Download entire file at once instead of streaming"
    ),
):
    """Download a file from a URL."""

    async def download():
        # Parse range string if provided
        range_start, range_end = None, None
        if range_str:
            if start is not None or end is not None:
                typer.echo("Cannot specify both --range and --start/--end", err=True)
                raise typer.Exit(1)

            try:
                if "-" in range_str:
                    parts = range_str.split("-", 1)
                    if parts[0]:
                        range_start = int(parts[0])
                    if parts[1]:
                        range_end = int(parts[1])
                else:
                    range_start = int(range_str)
            except ValueError:
                typer.echo(f"Invalid range format: {range_str}", err=True)
                raise typer.Exit(1)
        else:
            range_start, range_end = start, end

        async with AsyncClient() as client:
            blob = UrlBlob(url, client, url_type=state.url_type)

            # Use streaming to process data as it's downloaded
            total_bytes = 0
            line_count = 0

            if no_stream:
                # Use get() instead of stream()
                content = await blob.get(start=range_start, end=range_end)
                total_bytes = len(content)

                if output:
                    with open(output, "wb") as f:
                        f.write(content)
                    typer.echo(f"Downloaded {total_bytes} bytes to {output}", err=True)
                else:
                    # Try to decode as text for stdout, fallback to reporting size for binary
                    try:
                        sys.stdout.write(content.decode())
                    except UnicodeDecodeError:
                        typer.echo(
                            f"Downloaded {total_bytes} bytes of binary data (use -o to save to file)",
                            err=True,
                        )
            elif lines:
                # Process as lines of text
                if output:
                    with open(output, "w", encoding="utf-8") as f:
                        async for line in blob.stream_lines(
                            start=range_start, end=range_end
                        ):
                            f.write(line + "\n")
                            line_count += 1
                    typer.echo(f"Downloaded {line_count} lines to {output}", err=True)
                else:
                    async for line in blob.stream_lines(
                        start=range_start, end=range_end
                    ):
                        sys.stdout.write(line + "\n")
                        line_count += 1
            else:
                # Process as raw bytes
                if output:
                    with open(output, "wb") as f:
                        async for chunk in blob.stream(
                            start=range_start, end=range_end
                        ):
                            f.write(chunk)
                            total_bytes += len(chunk)
                    typer.echo(f"Downloaded {total_bytes} bytes to {output}", err=True)
                else:
                    # For stdout, collect chunks to try decoding as text at the end
                    chunks = []
                    async for chunk in blob.stream(start=range_start, end=range_end):
                        chunks.append(chunk)
                        total_bytes += len(chunk)

                    content = b"".join(chunks)
                    # Try to decode as text for stdout, fallback to reporting size for binary
                    try:
                        sys.stdout.write(content.decode())
                    except UnicodeDecodeError:
                        typer.echo(
                            f"Downloaded {total_bytes} bytes of binary data (use -o to save to file)",
                            err=True,
                        )

    asyncio.run(download())


@app.command()
def stat(
    url: str = typer.Argument(..., help="URL to get information about"),
    json_output: bool = typer.Option(
        False, "--json", "-j", help="Output in JSON format"
    ),
):
    """Get information about a file at a URL."""

    async def get_stats():
        async with AsyncClient() as client:
            blob = UrlBlob(url, client, url_type=state.url_type)
            stats = await blob.stat()
            stats_dict = stats.to_dict()

            if json_output:
                # Output as JSON
                print(json.dumps(stats_dict, indent=2))
            else:
                # Output as a nice table using Rich
                console = Console()
                table = Table()

                table.add_column("Property", style="cyan")
                table.add_column("Value", style="green")

                for key, value in stats_dict.items():
                    # Format size in a human-readable way if it's the size property
                    if key == "size" and isinstance(value, int):
                        # Convert to KB, MB, GB as appropriate
                        if value < 1024:
                            formatted_value = f"{value} bytes"
                        elif value < 1024 * 1024:
                            formatted_value = f"{value / 1024:.2f} KB"
                        elif value < 1024 * 1024 * 1024:
                            formatted_value = f"{value / (1024 * 1024):.2f} MB"
                        else:
                            formatted_value = f"{value / (1024 * 1024 * 1024):.2f} GB"
                        table.add_row(key, f"{value} ({formatted_value})")
                    else:
                        table.add_row(key, str(value))

                console.print(table)

    asyncio.run(get_stats())


def main():
    app()


if __name__ == "__main__":
    main()
