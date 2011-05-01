package org.neo4j.lab.cypher

import commands._
import org.junit.Test
import org.junit.Assert._


/**
 * Created by Andres Taylor
 * Date: 5/1/11
 * Time: 10:36 
 */

class CypherParserTest {
  @Test def shouldParseEasiestPossibleQuery() {
    val query =
      "from (start) = NODE(1) select start"

    val expectedQuery = Query(Select(NodeOutput("start")), From(NodeById("start", 1)))

    val parser = new CypherParser()
    val executionTree = parser.parse(query).get

    assertEquals(expectedQuery, executionTree)
  }
}
