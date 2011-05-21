package org.neo4j.lab.cypher

import commands._
import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Direction


/**
 * Created by Andres Taylor
 * Date: 5/1/11
 * Time: 10:36 
 */

class CypherParserTest {
  def testQuery(query: String, expectedQuery: Query) {
    val parser = new CypherParser()
    val executionTree = parser.parse(query).get

    assertEquals(expectedQuery, executionTree)
  }

  @Test def shouldParseEasiestPossibleQuery() {
    testQuery(
      "start start = node(1) select start",
      Query(
        Select(EntityOutput("start")),
        Start(NodeById("start", 1)),
        matching = None,
        where = None
      ))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "start start = node(1,2,3) select start",
      Query(
        Select(EntityOutput("start")),
        Start(NodeById("start", 1, 2, 3)),
        matching = None,
        where = None
      ))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "start a = node(1), b = node(2) select a,b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1), NodeById("b", 2)),
        matching = None,
        where = None
      ))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "start a = node(1) where a.name = \"andres\" select a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeById("a", 1)),
        matching = None,
        Some(Where(StringEquals("a", "name", "andres"))))
    )
  }

  @Test def multipleFilters() {
    testQuery(
      "start a = node(1) where a.name = \"andres\" or a.name = \"mattias\" select a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeById("a", 1)),
        None,
        Some(Where(
          Or(
            StringEquals("a", "name", "andres"),
            StringEquals("a", "name", "mattias")
          ))))
    )
  }

  @Test def relatedTo() {
    testQuery(
      "start a = node(1) match (a) -[:KNOWS]-> (b) select a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Some(Match(RelatedTo("a", "b", None, Some("KNOWS"), Direction.OUTGOING))),
        None
      )
    )
  }

  @Test def relatedToWithoutRelType() {
    testQuery(
      "start a = node(1) match (a) --> (b) select a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Some(Match(RelatedTo("a", "b", None, None, Direction.OUTGOING))),
        None
      )
    )
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      "start a = node(1) match (a) <-[:KNOWS]- (b) select a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Some(Match(RelatedTo("a", "b", None, Some("KNOWS"), Direction.INCOMING))),
        None
      )
    )
  }

  @Test def shouldOutputVariables() {
    testQuery(
      "start a = node(1) select a.name",
      Query(
        Select(PropertyOutput("a", "name")),
        Start(NodeById("a", 1)),
        None,
        None)
    )
  }

  @Test def shouldHandleAndClauses() {
    testQuery(
      "start a = node(1) where a.name = \"andres\" and a.lastname = \"taylor\" select a.name",
      Query(
        Select(PropertyOutput("a", "name")),
        Start(NodeById("a", 1)),
        None,
        Some(Where(And(StringEquals("a", "name", "andres"), StringEquals("a", "lastname", "taylor"))))
      )
    )
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      "start a = node(1) match (a) -[rel,:KNOWS]-> (b) select rel",
      Query(
        Select(EntityOutput("rel")),
        Start(NodeById("a", 1)),
        Some(Match(RelatedTo("a", "b", Some("rel"), Some("KNOWS"), Direction.OUTGOING))),
        None
      )
    )
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      "start a = node(1) match (a) -[:MARRIED]-> () select a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Some(Match(RelatedTo("a", "___NODE1", None, Some("MARRIED"), Direction.OUTGOING))),
        None
      )
    )
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      "start a = node(1) match (a) -[:KNOWS]-> (b) -[:FRIEND]-> (c) select c",
      Query(
        Select(EntityOutput("c")),
        Start(NodeById("a", 1)),
        Some(Match(
          RelatedTo("a", "b", None, Some("KNOWS"), Direction.OUTGOING),
          RelatedTo("b", "c", None, Some("FRIEND"), Direction.OUTGOING))),
        None
      )
    )
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      "start a = node_index(\"index\", \"key\", \"value\") select a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeByIndex("a", "index", "key", "value")),
        None,
        None
      )
    )
  }
}
