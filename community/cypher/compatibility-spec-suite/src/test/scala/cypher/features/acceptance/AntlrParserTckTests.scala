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
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.cst.factory.neo4j.ast.CypherAstParser
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.DenylistEntry
import org.neo4j.cypher.internal.util.test_helpers.FeatureQueryTest
import org.neo4j.cypher.internal.util.test_helpers.FeatureTest
import org.opencypher.tools.tck.api.Scenario
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class AntlrParserTckTests extends FeatureTest with FeatureQueryTest with Matchers {

  override val scenarios: Seq[Scenario] =
    BaseFeatureTestHolder.allAcceptanceScenarios ++ BaseFeatureTestHolder.allTckScenarios

  override def denylist(): Seq[DenylistEntry] = Seq.empty

  override def runQuery(scenario: Scenario, query: String): Option[Executable] = {
    Some(() => compareJavaCcAndAntlr(query))
  }

  private def compareJavaCcAndAntlr(query: String): Unit = {
    val javaCcAst = Try(parseJavaCc(query))
    val antlrAst = Try(parseAntlr(query))
    if (javaCcAst.isSuccess) {
      // TODO Compare error messages once error handling is properly implemented
      if (antlrAst != javaCcAst) {
        val normalisedAntlr = antlrAst.endoRewrite(Normaliser)
        val normalisedJavaCc = javaCcAst.endoRewrite(Normaliser)
        if (normalisedAntlr != normalisedJavaCc) {
          fail(
            s"""Query:
               |$query
               |Java CC: ${normaliseToString(javaCcAst)}
               |  ANTLR: ${normaliseToString(antlrAst)}
               |
               |Java CC norm: ${normaliseToString(normalisedJavaCc)}
               |  ANTLR norm: ${normaliseToString(normalisedAntlr)}
               |""".stripMargin
          )
        }
      }
    }
  }

  private def parseJavaCc(query: String): Statement = {
    JavaCCParser.parse(query, OpenCypherExceptionFactory(None))
  }

  private def parseAntlr(query: String): Statement = {
    CypherAstParser.parseStatements(query, Neo4jCypherExceptionFactory(query, None), None)
  }

  private def normaliseToString(ast: Any): String = ast.toString
    .replace("ArraySeq(", "Seq(")
    .replace("List(", "Seq(")
    .replace("Vector(", "Seq(")

  override def releaseResources(): Unit = {}
}

object Normaliser extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case or @ Or(Or(innerLhs, innerRhs), rhs) => Ors(ListSet(innerLhs, innerRhs, rhs))(or.position)
    case or @ Or(Ors(inner), rhs)             => Ors(inner.incl(rhs))(or.position)
  })

  override def apply(v: AnyRef): AnyRef = {
    instance.apply(v)
  }
}
