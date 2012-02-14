Idea for a rate limiting implementation in ruby:

- Count events in buckets of configurable width (i.e. 60 seconds)
- Aggregate total by looking at last n buckets
- Use key/value store that supports increment/multiget to be efficient
