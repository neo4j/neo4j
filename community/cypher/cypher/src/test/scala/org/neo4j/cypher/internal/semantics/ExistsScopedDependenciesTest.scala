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
package org.neo4j.cypher.internal.semantics

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.monitoring.Monitors

class DummyException() extends RuntimeException

class ExistsScopedDependenciesTest extends CypherFunSuite with AstConstructionTestSupport {

  private val parsing = CompilationPhases.parsing(ParsingConfig(CypherVersion.Default)) andThen Namespacer

  private val dummyExceptionFactory = new CypherExceptionFactory {
    override def arithmeticException(message: String, cause: Exception): RuntimeException = new DummyException
    override def syntaxException(message: String, pos: InputPosition): RuntimeException = new DummyException
  }

  val context = new BaseContext {
    override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
    override def notificationLogger: InternalNotificationLogger = devNullLogger
    override def cypherExceptionFactory: CypherExceptionFactory = dummyExceptionFactory
    override def monitors: phases.Monitors = WrappedMonitors(mock[Monitors])

    override def errorHandler: Seq[SemanticErrorDef] => Unit =
      errs => if (errs.nonEmpty) throw new Exception(s"had the following errors $errs")
    override val errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
    override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled
    override def internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStatsNoOp

    override def sessionDatabase: DatabaseReference = null
  }

  test(
    "Simple exists sets the scoped dependencies and introduced variables correctly"
  ) {
    val queryString = """MATCH (m)
                        |WHERE
                        | EXISTS {
                        |   MATCH (n)
                        |   WHERE
                        |     ALL(i in n.prop WHERE i = 4)
                        |     AND
                        |     EXISTS { (p)-->(m) }
                        | }
                        |RETURN m""".stripMargin

    val state = InitialState(
      queryString,
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )

    val parsed = parsing.transform(state, context).statement()

    val existsExpressions = parsed.folder.findAllByClass[ExistsExpression]

    val outerExists = existsExpressions(0)
    val nestedExists = existsExpressions(1)

    outerExists.introducedVariables shouldEqual Set(varFor("n"), varFor("i"), varFor("p"))
    outerExists.scopeDependencies shouldEqual Set(varFor("m"))
    nestedExists.introducedVariables shouldEqual Set(varFor("p"))
    nestedExists.scopeDependencies shouldEqual Set(varFor("m"))
  }

  test(
    "Full exists sets the scoped dependencies and introduced variables correctly"
  ) {
    val queryString =
      """WITH 4 AS x
        |MATCH (m)
        |WHERE EXISTS {
        |   WITH x AS y
        |   MATCH (n) WHERE ALL(i in n.prop WHERE i = y) AND
        |   EXISTS { MATCH (p)-->(m) WHERE p.prop = y RETURN p }
        |   RETURN n
        |}
        |RETURN m""".stripMargin

    val state = InitialState(
      queryString,
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )

    val parsed = parsing.transform(state, context).statement()

    val existsExpressions = parsed.folder.findAllByClass[ExistsExpression]
    val outerExists = existsExpressions(0)
    val nestedExists = existsExpressions(1)

    outerExists.introducedVariables shouldEqual Set(varFor("y"), varFor("n"), varFor("i"), varFor("p"))
    outerExists.scopeDependencies shouldEqual Set(varFor("x"), varFor("m"))
    nestedExists.introducedVariables shouldEqual Set(varFor("p"))
    nestedExists.scopeDependencies shouldEqual Set(varFor("m"), varFor("y"))
  }

  test(
    "Full exists sets the scoped dependencies and introduced variables correctly, with WITH in nested EXISTS"
  ) {
    val queryString =
      """WITH 4 AS x
        |MATCH (m)
        |WHERE EXISTS {
        |   MATCH (n) WHERE ALL(i in n.prop WHERE i = 4) AND
        |   EXISTS { WITH x AS y MATCH (p)-->(m) WHERE p.prop = y RETURN p }
        |   RETURN n
        |}
        |RETURN m""".stripMargin

    val state = InitialState(
      queryString,
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )

    val parsed = parsing.transform(state, context).statement()

    val existsExpressions = parsed.folder.findAllByClass[ExistsExpression]
    val outerExists = existsExpressions(0)
    val nestedExists = existsExpressions(1)

    outerExists.introducedVariables shouldEqual Set(varFor("n"), varFor("y"), varFor("p"), varFor("i"))
    outerExists.scopeDependencies shouldEqual Set(varFor("x"), varFor("m"))
    nestedExists.introducedVariables shouldEqual Set(varFor("y"), varFor("p"))
    nestedExists.scopeDependencies shouldEqual Set(varFor("x"), varFor("m"))
  }

  test(
    "Full exists sets the scoped dependencies and introduced variables correctly, with WITHs everywhere"
  ) {
    val queryString =
      """WITH 4 AS x
        |MATCH (m)
        |WHERE EXISTS {
        |   WITH x AS y
        |   MATCH (n) WHERE ALL(i in n.prop WHERE i = y) AND
        |   EXISTS { WITH y AS z MATCH (p)-->(m) WHERE p.prop = z RETURN p }
        |   RETURN n
        |}
        |RETURN m""".stripMargin

    val state = InitialState(
      queryString,
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )

    val parsed = parsing.transform(state, context).statement()

    val existsExpressions = parsed.folder.findAllByClass[ExistsExpression]
    val outerExists = existsExpressions(0)
    val nestedExists = existsExpressions(1)

    outerExists.introducedVariables shouldEqual Set(varFor("y"), varFor("n"), varFor("z"), varFor("i"), varFor("p"))
    outerExists.scopeDependencies shouldEqual Set(varFor("x"), varFor("m"))
    nestedExists.introducedVariables shouldEqual Set(varFor("z"), varFor("p"))
    nestedExists.scopeDependencies shouldEqual Set(varFor("m"), varFor("y"))
  }

  test(
    "Full exists should allow renaming of outer variables"
  ) {
    val queryString =
      """WITH 4 AS x
        |MATCH (m)
        |WHERE EXISTS {
        |   WITH x AS y
        |   MATCH (n) WHERE ALL(i in n.prop WHERE i = y) AND
        |   EXISTS { WITH y AS q MATCH (p)-->(m) WHERE p.prop = q RETURN q }
        |   RETURN n
        |}
        |RETURN m""".stripMargin

    val state = InitialState(
      queryString,
      IDPPlannerName,
      new AnonymousVariableNameGenerator
    )

    val parsed = parsing.transform(state, context).statement()

    val existsExpressions = parsed.folder.findAllByClass[ExistsExpression]
    val outerExists = existsExpressions(0)
    val nestedExists = existsExpressions(1)

    outerExists.introducedVariables shouldEqual Set(varFor("p"), varFor("y"), varFor("n"), varFor("i"), varFor("q"))
    outerExists.scopeDependencies shouldEqual Set(varFor("m"), varFor("x"))
    nestedExists.introducedVariables shouldEqual Set(varFor("p"), varFor("q"))
    nestedExists.scopeDependencies shouldEqual Set(varFor("m"), varFor("y"))
  }
}
