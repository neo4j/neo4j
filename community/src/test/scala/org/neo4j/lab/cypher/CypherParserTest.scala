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
      "start s = node(1) return s",
      Query(
        Select(EntityOutput("s")),
        Start(NodeById("s", 1))
      ))
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      "start a = node_index(\"index\", \"key\", \"value\") return a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeByIndex("a", "index", "key", "value"))
      )
    )
  }


    @Test def keywordsShouldBeCaseInsensitive() {
    testQuery(
      "START start = NODE(1) RETURN start",
      Query(
        Select(EntityOutput("start")),
        Start(NodeById("start", 1))
      ))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "start s = node(1,2,3) return s",
      Query(
        Select(EntityOutput("s")),
        Start(NodeById("s", 1, 2, 3))
      ))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "start a = node(1), b = node(2) return a,b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1), NodeById("b", 2))
      ))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "start a = node(1) where a.name = \"andres\" return a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Where(StringEquals("a", "name", "andres")))
    )
  }

  @Test def multipleFilters() {
    testQuery(
      "start a = node(1) where a.name = \"andres\" or a.name = \"mattias\" return a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Where(
          Or(
            StringEquals("a", "name", "andres"),
            StringEquals("a", "name", "mattias")
          )))
    )
  }

  @Test def relatedTo() {
    testQuery(
      "start a = node(1) match (a) -[:KNOWS]-> (b) return a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, Some("KNOWS"), Direction.OUTGOING))
      )
    )
  }

  @Test def relatedToWithoutRelType() {
    testQuery(
      "start a = node(1) match (a) --> (b) return a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, None, Direction.OUTGOING))
      )
    )
  }

  @Test def relatedToWithoutRelTypeButWithRelVariable() {
    testQuery(
      "start a = node(1) match (a) -[r]-> (b) return r",
      Query(
        Select(EntityOutput("r")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", Some("r"), None, Direction.OUTGOING))
      )
    )
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      "start a = node(1) match (a) <-[:KNOWS]- (b) return a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, Some("KNOWS"), Direction.INCOMING))
      )
    )
  }

  @Test def shouldOutputVariables() {
    testQuery(
      "start a = node(1) return a.name",
      Query(
        Select(PropertyOutput("a", "name")),
        Start(NodeById("a", 1)))
    )
  }

  @Test def shouldHandleAndClauses() {
    testQuery(
      "start a = node(1) where a.name = \"andres\" and a.lastname = \"taylor\" return a.name",
      Query(
        Select(PropertyOutput("a", "name")),
        Start(NodeById("a", 1)),
        Where(And(StringEquals("a", "name", "andres"), StringEquals("a", "lastname", "taylor")))
      )
    )
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      "start a = node(1) match (a) -[rel,:KNOWS]-> (b) return rel",
      Query(
        Select(EntityOutput("rel")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", "rel", "KNOWS", Direction.OUTGOING))
      )
    )
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      "start a = node(1) match (a) -[:MARRIED]-> () return a",
      Query(
        Select(EntityOutput("a")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "___NODE1", None, Some("MARRIED"), Direction.OUTGOING))
      )
    )
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      "start a = node(1) match (a) -[:KNOWS]-> (b) -[:FRIEND]-> (c) return c",
      Query(
        Select(EntityOutput("c")),
        Start(NodeById("a", 1)),
        Match(
          RelatedTo("a", "b", None, Some("KNOWS"), Direction.OUTGOING),
          RelatedTo("b", "c", None, Some("FRIEND"), Direction.OUTGOING))
      )
    )
  }

  @Test def countTheNumberOfHits() {
    testQuery(
      "start a = node(1) match (a) --> (b) return a, b, count(*)",
      Query(
        Select(EntityOutput("a"), EntityOutput("b"), Count("*")),
        Start(NodeById("a", 1)),
        Match(RelatedTo("a", "b", None, None, Direction.OUTGOING)))
    )
  }

}
