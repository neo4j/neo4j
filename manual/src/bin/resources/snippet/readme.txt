Asciidoc code snippets
======================

Install snippet support:
Copy the snippet.py and snippet.conf files to ~/.asciidoc/filters/snippet
If needed, set permissions to execute snippet.py.

Make sure that the depency for the code to include is declared in the manual.
Example:

    <dependency>
      <groupId>org.neo4j.examples</groupId>
      <artifactId>neo4j-examples</artifactId>
      <version>${neo4j.version}</version>
      <classifier>test-sources</classifier>
      <scope>provided</scope>
    </dependency>

Then you can include a code snippet this way from an Asciidoc document:

.Ordered path example
[snippet,java]
----
component=neo4j-examples
source=org/neo4j/examples/orderedpath/OrderedPathTest.java
tag=walkOrderedPath
----

If the classifier of the artifact isn't "test-sources", set it using for example:
classifier=sources

How to tag the code:

Java syntax:

    // START SNIPPET: createReltype
    private static enum ExampleRelationshipTypes implements RelationshipType
    {
        EXAMPLE
    }
    // END SNIPPET: createReltype

XML syntax:

  <!-- START SNIPPET: deps -->
  <dependencies>
    <dependency>
      <groupId>org.neo4j</groupId>
      <artifactId>neo4j</artifactId>
      <version>${project.version}</version> <!-- for example 1.2.M03 -->
      <type>pom</type>
    </dependency>
  </dependencies>
  <!-- END SNIPPET: deps -->

