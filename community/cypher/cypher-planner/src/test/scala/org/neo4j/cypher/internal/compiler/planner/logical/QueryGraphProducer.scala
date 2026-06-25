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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionHelpers.randomVersion
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.TestSignatureResolvingPlanContext
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.ResolveCallablesFromPlanContext
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.frontend.phases.ASTRewriter
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.collapseMultipleInPredicates
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode

trait QueryGraphProducer {
  self: LogicalPlanningTestSupport =>

  // Hack to guarantee coverage of all versions :/
  def producePlannerQueryForPattern(
    query: String,
    appendReturn: Boolean = true
  ): (SinglePlannerQuery, SemanticTable) = producePlannerQueryForPattern(randomVersion(), query, appendReturn)

  /**
   * Returns a planner query and semantic table for the given query.
   * The plan context defines one procedure:
   *
   * my.proc.foo(a: INT): (x: INT, y: NODE)
   */
  def producePlannerQueryForPattern(
    version: CypherVersion,
    query: String,
    appendReturn: Boolean
  ): (SinglePlannerQuery, SemanticTable) = {
    val appendix = if (appendReturn) " RETURN 1 AS Result" else ""
    val q = query + appendix
    val exceptionFactory = Neo4jCypherExceptionFactory(q, None)
    val ast = parse(q, exceptionFactory)
    val cleanedStatement: Statement =
      ast.endoRewrite(inSequence(NormalizeWithAndReturnClauses(exceptionFactory, Some(version))))
    val onError = SyntaxExceptionCreator.throwOnError(exceptionFactory)
    val semanticContext = SemanticCheckContext(version, NotImplementedErrorMessageProvider)
    val SemanticCheckResult(semanticState, errors) =
      SemanticChecker.check(cleanedStatement, SemanticState.clean.withFeatures(semanticFeatures), semanticContext)
    onError(errors)

    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName(ns, "foo")(pos)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

    val signature =
      ProcedureSignature(name, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
    val procLookup: ProcedureName => ProcedureSignature = _ => signature
    val fcnLookup: FunctionName => Option[UserFunctionSignature] = _ => None
    val resolver = new TestSignatureResolvingPlanContext(procLookup, fcnLookup)

    val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
    // if you ever want to have parameters in here, fix the map
    val firstRewriteStep =
      ASTRewriter.rewrite(
        cleanedStatement,
        semanticState,
        Map.empty,
        exceptionFactory,
        anonymousVariableNameGenerator,
        CancellationChecker.NeverCancelled,
        version
      )
    val state = LogicalPlanState(
      query,
      IDPPlannerName,
      newStubbedPlanningAttributes,
      anonymousVariableNameGenerator,
      Some(resolver.procedureSignatureVersion),
      Some(firstRewriteStep),
      None,
      None,
      Some(semanticState)
    )
    val context = ContextHelper.create(
      version = version,
      logicalPlanIdGen = idGen,
      planContext = resolver,
      semanticFeatures = semanticFeatures
    )
    val output = (
      ResolveCallablesFromPlanContext andThen
        SemanticAnalysis(warn = Some(false)) andThen
        ScopeSurveyor andThen
        Namespacer andThen
        rewriteEqualityToInPredicate andThen
        cnfNormalizerTransformer andThen
        collapseMultipleInPredicates
    ).transform(state, context)

    val semanticTable = output.semanticTable()
    val plannerQuery =
      StatementConverters.withDefaultConfig.convertToPlannerQuery(
        output.statement().asInstanceOf[Query],
        semanticTable,
        anonymousVariableNameGenerator,
        CancellationChecker.NeverCancelled
      )
    (plannerQuery.asInstanceOf[SinglePlannerQuery], semanticTable)
  }
}
