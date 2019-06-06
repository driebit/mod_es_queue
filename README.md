# mod_es_queue
====================

## Installation
------------
Add [tsloughter/erlastic_search](https://github.com/tsloughter/erlastic_search) and its
dependencies at their proper version (for Erlang 18) to your Zotonic deps in `zotonic.config`:

```erlang
{deps, [
    %% ...
    {erlastic_search, ".*", {git, "https://github.com/tsloughter/erlastic_search.git", {tag, "master"}}},
    {hackney, ".*", {git, "https://github.com/benoitc/hackney.git", {tag, "1.6.1"}}},
    {jsx, ".*", {git, "https://github.com/talentdeficit/jsx.git", {tag, "2.8.0"}}}
]}
```

## Usage

mod_es_queue exports only two functions:

### start_link/1

Starts the gen_server. Arguments are ignored, but Zotonic requires a single argument for it's modules

### insert/4

Arguments
1. Index : binary() -> The index into which the document is to be inserted
2. Type : binary() -> The ElasticSearch type
3. Id : binary() -> The ID of the document to be inserted
4. Doc : map() -> The document to insert

The inserted document will be placed into a queue. Once either the queue is full (currently hardcoded at 500 documents) or no new documents have been inserted in 5 seconds, the contents of the queue will be bulk-inserted into ElasticSearch
