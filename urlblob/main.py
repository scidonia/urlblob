import sys
import asyncio
import typer
from typing import Optional
from httpx import AsyncClient
from urlblob.blob import UrlBlob

app = typer.Typer(help="URL Blob - A tool for working with URL data")


@app.command()
def put(
    url: str = typer.Argument(..., help="URL to upload to"),
    content: Optional[str] = typer.Argument(
        None, help="Content to upload (omit to read from stdin)"
    ),
    content_type: Optional[str] = typer.Option(
        "text/plain", "--content-type", "-t", help="Content type of the data"
    ),
):
    """Upload content to a URL."""
    if content is None:
        # Read from stdin if no content provided
        content = sys.stdin.read()

    typer.echo(f"Uploading to {url} with content type: {content_type}")
    typer.echo(f"Content: {content[:50]}{'...' if len(content) > 50 else ''}")
    # Implementation will go here


@app.command()
def get(
    url: str = typer.Argument(..., help="URL to download from"),
    range_str: Optional[str] = typer.Argument(
        None,
        help="Byte range in format 'start-end', 'start-', or '-end'",
    ),
    start: Optional[int] = typer.Option(None, help="Start byte position"),
    end: Optional[int] = typer.Option(None, help="End byte position"),
    output: Optional[str] = typer.Option(
        None, "--output", "-o", help="Output file (default: stdout)"
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
            blob = UrlBlob(url, client)
            content = await blob.get(start=range_start, end=range_end)

            if output:
                with open(output, "wb") as f:
                    f.write(content)
                typer.echo(f"Downloaded {len(content)} bytes to {output}", err=True)
            else:
                # Try to decode as text for stdout, fallback to reporting size for binary
                try:
                    sys.stdout.write(content.decode())
                except UnicodeDecodeError:
                    typer.echo(
                        f"Downloaded {len(content)} bytes of binary data (use -o to save to file)",
                        err=True,
                    )

    asyncio.run(download())


@app.command()
def stat(url: str = typer.Argument(..., help="URL to get information about")):
    """Get information about a file at a URL."""
    typer.echo(f"Getting information about {url}")
    # Implementation will go here


def main():
    app()


if __name__ == "__main__":
    main()
