# URL Blob

A library for providing agnostic access to presigned URLs living at different cloud providers.

## Library Usage

The UrlBlob library provides a consistent interface for working with files stored in various cloud providers through presigned URLs.

## Command Line Interface

The library also includes a CLI for convenient access to its functionality:

### Upload content to a URL

```bash
# Upload content directly from command line
urlblob put https://example.com/path "Hello, world!"

# Upload content from stdin
echo "Hello, world!" | urlblob put https://example.com/path

# Specify content type
urlblob put https://example.com/path "{'key': 'value'}" --content-type application/json
```

Options:
- `--content-type TEXT`, `-t TEXT`: Content type of the data (default: text/plain)

### Download a file

```bash
# Download entire file
urlblob get https://example.com/path/to/file

# Download with byte range
urlblob get https://example.com/path/to/file 0-1024
urlblob get https://example.com/path/to/file 1024-
urlblob get https://example.com/path/to/file -1024

# Save to file instead of stdout
urlblob get https://example.com/path/to/file -o output.txt
```

### Get file information

```bash
urlblob stat https://example.com/path/to/file
```
