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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.generator.AstGenerator
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleGraphs
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsMultipleGraphsSelector
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.defaultSemanticFeatures
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.frontend.phases
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.devNullLogger
import org.neo4j.cypher.internal.util.helpers.NameDeduplicator.UNNAMED_PATTERN
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.CypherScalaCheckDrivenPropertyChecks
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.monitoring.Monitors

class FabricParsingPropertyTest extends CypherFunSuite
    with CypherScalaCheckDrivenPropertyChecks
    with AstConstructionTestSupport {

  private val astGenerator = new AstGenerator(simpleStrings = false)

  implicit val config: PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 500)

  private val resolver = {
    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature =
      ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)

    new ProcedureSignatureResolver {
      override def procedureSignature(name: QualifiedName): ProcedureSignature = signature
      override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = None
      override def procedureSignatureVersion: Long = -1
    }
  }

  private val fabricParsingConfig =
    ParsingConfig(semanticFeatures = defaultSemanticFeatures ++ Seq(MultipleGraphs, UseAsMultipleGraphsSelector))
  private val fabricParsing = CompilationPhases.fabricParsing(fabricParsingConfig, resolver)

  private val prettifier: Prettifier =
    Prettifier(ExpressionStringifier(alwaysParens = true, alwaysBacktick = true, sensitiveParamsAsParams = true))

  class DummyException() extends Exception

  private val dummyExceptionFactory = new CypherExceptionFactory {
    override def arithmeticException(message: String, cause: Exception): Exception = new DummyException
    override def syntaxException(message: String, pos: InputPosition): Exception = new DummyException
  }

  // The result of fabricParsing gets prettified later on in FabricStitcher.
  // The resulting string gets sent to a remote instance.
  // This string must may contain anonymous variable names, but they must use negative numbers.
  // Otherwise newly generated anonymous variable names
  // on the remote instance can clash with the existing anonymous variable names.
  test("fabricParsing should not introduce anonymous variable names with non-negative numbers.") {
    // To reproduce test failures, enable the following line with the seed from the TC build
    // setScalaCheckInitialSeed(seed)
    forAll(astGenerator._statement) { statement =>
      val queryString = prettifier.asString(statement)
      withClue(s"Original queryString: $queryString\n") {
        val state = InitialState(
          queryString,
          None,
          IDPPlannerName,
          new AnonymousVariableNameGenerator(negativeNumbers = true)
        )

        val context = new BaseContext {
          override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING
          override def notificationLogger: InternalNotificationLogger = devNullLogger
          override def cypherExceptionFactory: CypherExceptionFactory = dummyExceptionFactory
          override def monitors: phases.Monitors = WrappedMonitors(mock[Monitors])
          // Ignore semantic errors
          override def errorHandler: Seq[SemanticErrorDef] => Unit = _ => ()
          override val errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
          override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled
          override def internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStatsNoOp

          override def targetsComposite: Boolean = false
          override def sessionDatabaseName: String = "ignore"
        }

        try {
          val fabricParsed = fabricParsing.transform(state, context).statement()
          val rewrittenQueryString = prettifier.asString(fabricParsed)
          withClue(s"Rewritten queryString: $rewrittenQueryString\n") {
            UNNAMED_PATTERN.findAllMatchIn(rewrittenQueryString).foreach(
              _.group(2).toInt should be < 0
            )
          }
        } catch {
          case _: DummyException =>
          // Ignore. We can get those for certain semantic errors caught by the rewriters.
          case _: IllegalStateException  =>
          case _: NoSuchElementException =>
          // Ignore. We can reach invalid states by ignoring semantic errors and continuing.
        }
      }
    }
  }
}
