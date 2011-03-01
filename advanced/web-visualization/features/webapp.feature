Feature: Neo4j Visualization web component
  In order to have some idea about the data within a graph
  As a Neo4j application developer
  I want to see a visualization of the graph using a web browser

  Scenario: See the visualization component in a browser
    Given a local web server hosting the visualization component
    When I look at the neo4j-visualization page
    Then the page should show me a pretty graph

