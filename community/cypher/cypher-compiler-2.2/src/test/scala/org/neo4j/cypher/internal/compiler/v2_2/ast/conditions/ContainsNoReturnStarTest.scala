package org.neo4j.cypher.internal.compiler.v2_2.ast.conditions

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast._

class ContainsNoReturnStarTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Happy when not finding ReturnAll()") {
    val ast: ASTNode = Match(optional = false, Pattern(Seq(EveryPath(NodePattern(None, Seq(), None, naked = true)_)))_, Seq(), None)_

    containsNoReturnStar(ast) should equal(Seq())
  }

  test("Fails when finding ReturnAll()") {
    val ast: ASTNode = Return(false, ReturnAll()_, None, None, None)_

    containsNoReturnStar(ast) should equal(Seq("Expected none but found ReturnAll at position line 1, column 0"))
  }
}
