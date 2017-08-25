package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.SemanticState
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class SingleGraphItemTest extends CypherFunSuite with AstConstructionTestSupport {

  test("Self alias does not produce a semantic error") {

    val Right(state) = SemanticState.clean.withFeature('multigraph).declareGraphVariable(varFor("foo"))

    val result = graphAs("foo", "foo").semanticCheck(state)
    val errors = result.errors.toSet

    errors.isEmpty should be(true)
  }
}
