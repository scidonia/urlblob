# urlblob - work with presigned URLs from various cloud providers
This library implements an agnostic way of working with pre-signed URLs from different cloud providers, in order to support stat, get, and put. Support for multi-part upload and delete is also planned.

All the major cloud providers offer a way to hand out URLs to objects in a bucket (or bucket-equivalent), allowing URL users to work with these objects without havign to authenticate themselves. This would be great for cloud-agnostic processing applications, except that different cloud providers sometimes do things slightly differently. This library papers over those differences to provide you with a truly cloud-agnostic way of working with blobs behind these URLs.
