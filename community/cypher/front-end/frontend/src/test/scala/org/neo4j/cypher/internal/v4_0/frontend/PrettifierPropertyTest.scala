package org.neo4j.cypher.internal.v4_0.frontend

import org.neo4j.cypher.internal.v4_0.ast.generator.AstGenerator
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.parser.CypherParser
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class PrettifierPropertyTest extends CypherFunSuite
  with GeneratorDrivenPropertyChecks
  with PrettifierTestUtils {

  val pr = Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true))

  val parser = new CypherParser

  val gen = AstGenerator(simpleStrings = false)

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

  test("Prettifier output should parse to the same ast") {
    forAll(gen._query) { query =>
      roundTripCheck(query)
    }
  }
}
