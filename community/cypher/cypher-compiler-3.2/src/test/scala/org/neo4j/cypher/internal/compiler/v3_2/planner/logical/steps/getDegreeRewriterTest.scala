package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.steps

import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.ast.functions.Exists
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class getDegreeRewriterTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Rewrite exists( (a)-[:FOO]->() ) to GetDegree( (a)-[:FOO]->() ) > 0") {
    val incoming = Exists.asInvocation(
      PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(varFor("a")), Seq.empty, None)(pos),
                                                               RelationshipPattern(None, Seq(RelTypeName("FOO")(pos)), None, None, SemanticDirection.OUTGOING)(pos),
                                                               NodePattern(None, Seq.empty, None)(pos))(pos))(pos)))(pos)
    val expected = GreaterThan(GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.OUTGOING)(pos), SignedDecimalIntegerLiteral("0")(pos))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }

  test("Rewrite exists( ()-[:FOO]->(a) ) to GetDegree( (a)<-[:FOO]-() ) > 0") {
    val incoming = Exists.asInvocation(
      PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, Seq.empty, None)(pos),
                                                               RelationshipPattern(None, Seq(RelTypeName("FOO")(pos)), None, None, SemanticDirection.OUTGOING)(pos),
                                                               NodePattern(Some(varFor("a")), Seq.empty, None)(pos))(pos))(pos)))(pos)
    val expected = GreaterThan(GetDegree(varFor("a"), Some(RelTypeName("FOO")(pos)), SemanticDirection.INCOMING)(pos), SignedDecimalIntegerLiteral("0")(pos))(pos)

    getDegreeRewriter(incoming) should equal(expected)
  }
}
