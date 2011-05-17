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
      "from start = node(1) select start",
      Query(
        Select(EntityOutput("start")),
        From(NodeById("start", 1))))
  }

  @Test def shouldParseMultipleNodes() {
    testQuery(
      "from start = node(1,2,3) select start",
      Query(
        Select(EntityOutput("start")),
        From(NodeById("start", 1, 2, 3))))
  }

  @Test def shouldParseMultipleInputs() {
    testQuery(
      "from a = node(1), b = node(2) select a,b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        From(NodeById("a", 1), NodeById("b", 2))))
  }

  @Test def shouldFilterOnProp() {
    testQuery(
      "from a = node(1) where a.name = \"andres\" select a",
      Query(
        Select(EntityOutput("a")),
        From(NodeById("a", 1)),
        Some(Where(StringEquals("a", "name", "andres"))))
    )
  }

  @Test def multipleFilters() {
    testQuery(
      "from a = node(1) where a.name = \"andres\" or a.name = \"mattias\" select a",
      Query(
        Select(EntityOutput("a")),
        From(NodeById("a", 1)),
        Some(Where(Or(
          StringEquals("a", "name", "andres"), StringEquals("a", "name", "mattias")
        ))))
    )
  }

  @Test def relatedTo() {
    testQuery(
      "from a = node(1) where (a) -['KNOWS']-> (b) select a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        From(NodeById("a", 1)),
        Some(Where(RelatedTo("a", "b", None, "KNOWS", Direction.OUTGOING)))
      )
    )
  }

  @Test def relatedToTheOtherWay() {
    testQuery(
      "from a = node(1) where (a) <-['KNOWS']- (b) select a, b",
      Query(
        Select(EntityOutput("a"), EntityOutput("b")),
        From(NodeById("a", 1)),
        Some(Where(RelatedTo("a", "b", None, "KNOWS", Direction.INCOMING)))
      )
    )
  }

  @Test def shouldOutputVariables() {
    testQuery(
      "from a = node(1) select a.name",
      Query(
        Select(PropertyOutput("a", "name")),
        From(NodeById("a", 1)))
    )
  }

  @Test def shouldHandleAndClauses() {
    testQuery(
      "from a = node(1) where a.name = \"andres\" and a.lastname = \"taylor\" select a.name",
      Query(
        Select(PropertyOutput("a", "name")),
        From(NodeById("a", 1)),
        Some(Where(And(StringEquals("a", "name", "andres"), StringEquals("a", "lastname", "taylor"))))
      )
    )
  }

  @Test def relatedToWithRelationOutput() {
    testQuery(
      "from a = node(1) where (a) -[rel,'KNOWS']-> (b) select rel",
      Query(
        Select(EntityOutput("rel")),
        From(NodeById("a", 1)),
        Some(Where(RelatedTo("a", "b", Some("rel"), "KNOWS", Direction.OUTGOING)))
      )
    )
  }

  @Test def relatedToWithoutEndName() {
    testQuery(
      "from a = node(1) where (a) -['MARRIED']-> () select a",
      Query(
        Select(EntityOutput("a")),
        From(NodeById("a", 1)),
        Some(Where(RelatedTo("a", "___NODE1", None, "MARRIED", Direction.OUTGOING)))
      )
    )
  }

  @Test def relatedInTwoSteps() {
    testQuery(
      "from a = node(1) where (a) -['KNOWS']-> (b) -['FRIEND']-> (c) select c",
      Query(
        Select(EntityOutput("c")),
        From(NodeById("a", 1)),
        Some(Where(
          And(
            RelatedTo("a", "b", None, "KNOWS", Direction.OUTGOING),
            RelatedTo("b", "c", None, "FRIEND", Direction.OUTGOING)
          )))
      )
    )
  }

  @Test def sourceIsAnIndex() {
    testQuery(
      "from a = node_index(\"index\", \"key\", \"value\") select a",
      Query(
        Select(EntityOutput("a")),
        From(NodeByIndex("a", "index", "key", "value"))
      )
    )
  }
}
