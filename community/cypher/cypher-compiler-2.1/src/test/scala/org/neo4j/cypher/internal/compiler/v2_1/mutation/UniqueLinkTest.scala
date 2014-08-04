package org.neo4j.cypher.internal.compiler.v2_1.mutation

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects._
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.graphdb.Direction

class UniqueLinkTest extends CypherFunSuite {
  test("given both end nodes, only claims to write relationships, not nodes") {
    val link = UniqueLink("a", "b", "r", "X", Direction.OUTGOING)
    val symbols = new SymbolTable(Map("a" -> CTNode, "b" -> CTNode))
    link.effects(symbols) should equal(READS_RELATIONSHIPS | WRITES_RELATIONSHIPS)
  }

  test("given one end, creates nodes and relationships") {
    val link = UniqueLink("a", "b", "r", "X", Direction.OUTGOING)
    val symbols = new SymbolTable(Map("a" -> CTNode))
    link.effects(symbols) should equal(ALL)
  }
}
