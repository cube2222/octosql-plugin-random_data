# OctoSQL Random Data Plugin

This is an example plugin for OctoSQL.

It exposes the following tables:
- addresses
- companies
- users

Underneath it uses the [Random Data API](https://random-data-api.com).

Releasing new versions is mostly automated by GoReleaser. Just push a tag and it will create a new release with all artifacts built. Then manually add the new version to the [octosql_manifest.json](octosql_manifest.json) file.
