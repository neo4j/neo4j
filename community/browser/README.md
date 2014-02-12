Neo4j Browser
=============

*Explore the graph, one query at a time.* 

Neo4j Browser is [Neo4j's](http://github.com/neo4j/neo4j/) built-in client application, a mashup of a REPL, data visualization and lightweight IDE.


Goals:

- developer focused
- every interaction is a demonstration
- uses only public APIs
- for modern browsers, with reasonable fallbacks


## Development

**Management**:

* [Trello Wall](https://trello.com/b/3QpahIAK/team-pop)

**How-to**:

* [Build instructions](https://github.com/neo4j/neo4j-browser/wiki/Build)

**Run in development mode**:

- run neo4j server
- npm install -g grunt-cli
- run grunt server in browser

This sets up localhost:9000 to run server and browser.  Any local changes will be applied here immediatley by reloading the page.

# Try it

At the moment, there is no ready-made download of Neo4j Browser. Until one is distributed, you'll need to [build it yourself](http://github.com/neo4j/neo4j-browser/wiki/Build).

## Feedback

* [Andreas Kollegger](mailto:andreas@neotechnology.com) - questions or comments? send an email to me
* [Github Issues](https://github.com/neo4j/neo4j-browser/issues?milestone=2&state=open) - feature requests and bug reports (please note OS + web browser)


## Adding a new Cypher keyword

Look at:

- Detecting Cypher: app/scripts/init/commandInterpreters.coffee
- app/components/cypher/index.js
- app/scripts/codemirror-cypher.coffee


