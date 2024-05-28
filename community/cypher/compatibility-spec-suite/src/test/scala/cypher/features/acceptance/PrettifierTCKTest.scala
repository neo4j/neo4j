/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package cypher.features.acceptance

import cypher.features.BaseFeatureTestHolder
import org.junit.jupiter.api.function.Executable
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.parser.v5.ast.factory.ast.CypherAstParser
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.test_helpers.DenylistEntry
import org.neo4j.cypher.internal.util.test_helpers.FeatureQueryTest
import org.neo4j.cypher.internal.util.test_helpers.FeatureTest
import org.opencypher.tools.tck.api.Scenario
import org.scalatest.matchers.should.Matchers

import scala.util.Success
import scala.util.Try

class PrettifierTCKTest extends PrettifierTCKTestBase {

  override protected def parseStatements(query: String): Statement =
    CypherAstParser.parseStatements(query, Neo4jCypherExceptionFactory(query, None), None)
}

class PrettifierJavaCcTCKTest extends PrettifierTCKTestBase {

  override protected def parseStatements(query: String): Statement = JavaCCParser.parse(
    query,
    OpenCypherExceptionFactory(None)
  )

  override def denylist(): Seq[DenylistEntry] = super.denylist() ++ Seq(
    // Bug in javacc parser
    """Feature "MiscAcceptance": Scenario "Github issue number 13432 query 1"""",
    """Feature "MiscAcceptance": Scenario "Github issue number 13432 query 2"""",
    """Feature "MiscAcceptance": Scenario "Github issue number 13432 query 3""""
  ).map(DenylistEntry.apply)
}

trait PrettifierTCKTestBase extends FeatureTest with FeatureQueryTest with Matchers {

  val prettifier: Prettifier = Prettifier(ExpressionStringifier(
    alwaysParens = true,
    alwaysBacktick = true,
    sensitiveParamsAsParams = true
  ))

  override val scenarios: Seq[Scenario] =
    BaseFeatureTestHolder.allAcceptanceScenarios ++ BaseFeatureTestHolder.allTckScenarios

  override def denylist(): Seq[DenylistEntry] = Seq(
    // Does not parse
    """Feature "Mathematical3 - Subtraction": Scenario "Fail for invalid Unicode hyphen in subtraction"""",
    """Feature "Literals7 - List": Scenario "Fail on a nested list with non-matching brackets"""",
    """Feature "Literals7 - List": Scenario "Fail on a list containing only a comma"""",
    """Feature "Literals7 - List": Scenario "Fail on a nested list with missing commas"""",
    """Feature "Literals2 - Decimal integer": Scenario "Fail on an integer containing a invalid symbol character"""",
    """Feature "Match4 - Match variable length patterns scenarios": Scenario "Fail on negative bound"""",
    """Feature "Match4 - Match variable length patterns scenarios": Scenario "Fail when asterisk operator is missing"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing only a comma"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing key starting with a number"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing key with dot"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing a list without key"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a nested map with non-matching braces"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing a value without key"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing a map without key"""",
    """Feature "Literals8 - Maps": Scenario "Fail on a map containing key with symbol"""",
    """Feature "Literals6 - String": Scenario "Failing on incorrect unicode literal"""",
    """Feature "GpmSyntaxMixingAcceptance": Scenario "Mixing QPP and var-length relationship quantifiers in pattern expressions in same statement - syntax error"""",

    // EXPLAIN is not covered by the Parser, but by the pre-parser
    """Feature "ExplainAcceptance": Scenario "Explanation of query without return columns"""",
    """Feature "ExplainAcceptance": Scenario "Explanation of standalone procedure call"""",
    """Feature "ExplainAcceptance": Scenario "Explanation of query with return columns"""",
    """Feature "ExplainAcceptance": Scenario "Explanation of in-query procedure call"""",
    """Feature "ExplainAcceptance": Scenario "Explanation of query ending in unit subquery call"""",

    // Relationship type expressions in RETURN AND WHERE clauses, and CASE expressions, is not yet implemented
    """Feature "QuantifiedPathPatternAcceptance": Scenario "Leaving out the nodes adjacent to a QPP"""",
    """Feature "QuantifiedPathPatternAcceptance": Scenario "Quantifier lower bound must be less than or equal to upper bound, upper bound needs to be positive": Example "1"""",

    // DIFFERENT NODES is not yet implemented
    """Feature "GpmSyntaxMixingAcceptance": Scenario "DIFFERENT NODES with var-length relationship - OK"""",
    """Feature "GpmSyntaxMixingAcceptance": Scenario "Explicit match mode DIFFERENT NODES with shortestPath - syntax error""""
  ).map(DenylistEntry(_))

  override def runQuery(scenario: Scenario, query: String): Option[Executable] = {
    val executable: Executable = () => roundTripCheck(query)
    Some(executable)
  }

  private def roundTripCheck(query: String): Unit = {
    val parsed = parse(query)
    val prettified = prettifier.asString(parsed)
    withClue(
      s"""
         |Query:
         |$query
         |Prettified Query:
         |$prettified
         |""".stripMargin
    ) {
      Try(parse(prettified)) shouldBe Success(parsed)
    }
  }

  protected def parseStatements(query: String): Statement

  private def parse(query: String): Statement = canonicalizeUnaliasedReturnItem(parseStatements(query))

  override def releaseResources(): Unit = {}

  /**
   * "RETURN a" might be round-tripped to "RETURN `a`"
   * This is an acceptable diversion caused by the Prettifier,
   * since it only can affect the final RETURN in a query and thus has no effect on [Fabric] subqueries.
   */
  private def canonicalizeUnaliasedReturnItem(statement: Statement): Statement = {
    statement.endoRewrite(bottomUp(Rewriter.lift {
      case x: UnaliasedReturnItem => x.copy(inputText = "")(x.position)
    }))
  }
}
