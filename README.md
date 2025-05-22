# URL Blob

A library for providing agnostic access to presigned URLs living at different cloud providers.

This library implements an agnostic way of working with pre-signed URLs from different cloud providers, in order to support stat, get, and put. Support for multi-part upload and delete is also planned.

All the major cloud providers offer a way to hand out URLs to objects in a bucket (or bucket-equivalent), allowing URL users to work with these objects without having to authenticate themselves. This would be great for cloud-agnostic processing applications, except that different cloud providers sometimes do things slightly differently. This library papers over those differences to provide you with a truly cloud-agnostic way of working with blobs behind these URLs.

## Library Usage

The UrlBlob library provides a consistent interface for working with files stored in various cloud providers through presigned URLs.

### Basic Usage (Async API)

```python
import asyncio
from urlblob.manager import UrlBlobManager

async def main():
    # Create a manager
    manager = UrlBlobManager()
    
    # Get a blob from a URL
    blob = manager.from_url("https://example.com/path/to/file")
    
    # Get file stats
    stats = await blob.stat()
    print(f"File size: {stats.size()} bytes")
    print(f"Content type: {stats.content_type()}")
    
    # Download content
    content = await blob.get()
    
    # Upload content
    await blob.put("Hello, world!", content_type="text/plain")

asyncio.run(main())
```

### Synchronous API

For applications that don't use async/await, UrlBlob provides a synchronous API with identical functionality:

```python
from urlblob import SyncUrlBlobManager

# Create a manager
manager = SyncUrlBlobManager()

# Get a blob from a URL
blob = manager.from_url("https://example.com/path/to/file")

# Get file stats
stats = blob.stat()
print(f"File size: {stats.size()} bytes")
print(f"Content type: {stats.content_type()}")

# Download content
content = blob.get()

# Upload content
blob.put("Hello, world!", content_type="text/plain")

# Use context manager for automatic cleanup
with SyncUrlBlobManager() as manager:
    blob = manager.from_url("https://example.com/path/to/file")
    # Work with blob...
```

### API Reference

#### UrlBlobManager

```python
# Initialize a manager
manager = UrlBlobManager()

# Create a blob from a URL with optional explicit URL type
blob = manager.from_url(url, url_type=None)
```

#### UrlBlob

```python
# Get file metadata
stats = await blob.stat()

# Download entire file
content = await blob.get()

# Download a byte range
# Note: byte_range is end-exclusive (like Python's range)
content = await blob.get(byte_range=range(0, 1024))  # Gets bytes 0-1023 (1024 bytes)

# While start/end parameters are end-inclusive
content = await blob.get(start=0, end=1023)  # Also gets bytes 0-1023 (1024 bytes)

# Stream file content
async for chunk in blob.stream():
    process(chunk)

# Stream file as lines of text
async for line in blob.stream_lines():
    process(line)

# Upload content (supports str, bytes, file objects, iterators)
await blob.put(content, content_type="text/plain")

# Upload content as lines
await blob.put_lines(["line1", "line2", "line3"], content_type="text/plain")
```

#### UrlBlobStats

```python
# Get file size in bytes
size = stats.size()
# or with None for missing size
size = stats.size_or_none()

# Get content type
content_type = stats.content_type()
# or with None for missing content type
content_type = stats.content_type_or_none()

# Get last modified timestamp
last_modified = stats.last_modified()
# or with None for missing timestamp
last_modified = stats.last_modified_or_none()

# Get all stats as a dictionary
stats_dict = stats.to_dict()
```

## Command Line Interface

The library also includes a CLI for convenient access to its functionality:

### Global Options

```bash
# Override URL type detection
urlblob --url-type s3 [command] [args]
```

Available URL types: `s3` (aliases: `aws`, `aws_s3`), `gcp` (aliases: `google`), `azure` (alias: `az`), `generic`

### Upload content to a URL

```bash
# Upload content directly from command line
urlblob put https://example.com/path "Hello, world!"

# Upload content from stdin
echo "Hello, world!" | urlblob put https://example.com/path

# Specify content type
urlblob put https://example.com/path "{'key': 'value'}" --content-type application/json

# Process input as lines of text
urlblob put https://example.com/path "line1
line2
line3" --lines
```

Options:

- `--content-type TEXT`, `-t TEXT`: Content type of the data (default: text/plain)
- `--lines`, `-l`: Process content as lines of text

### Download a file

```bash
# Download entire file
urlblob get https://example.com/path/to/file

# Download with byte range (ranges are inclusive for CLI, e.g. 0-1024 gets 1025 bytes)
urlblob get https://example.com/path/to/file 0-1024
urlblob get https://example.com/path/to/file 1024-
urlblob get https://example.com/path/to/file -1024

# Save to file instead of stdout
urlblob get https://example.com/path/to/file -o output.txt

# Process output as lines of text
urlblob get https://example.com/path/to/file --lines

# Download entire file at once (no streaming)
urlblob get https://example.com/path/to/file --no-stream
```

Options:

- `--output`, `-o`: Output file (default: stdout)
- `--lines`, `-l`: Process content as lines of text
- `--no-stream`: Download entire file at once instead of streaming
- `--start`: Start byte position
- `--end`: End byte position

### Get file information

```bash
# Get file stats in table format
urlblob stat https://example.com/path/to/file

# Get file stats in JSON format
urlblob stat https://example.com/path/to/file --json
```

Options:

- `--json`, `-j`: Output in JSON format
