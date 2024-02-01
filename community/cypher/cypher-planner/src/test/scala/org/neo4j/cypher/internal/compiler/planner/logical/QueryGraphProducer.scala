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

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticChecker
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.compiler.SyntaxExceptionCreator
import org.neo4j.cypher.internal.compiler.TestSignatureResolvingPlanContext
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.RewriteProcedureCalls
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.test_helpers.ContextHelper
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.frontend.phases.ASTRewriter
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.collapseMultipleInPredicates
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.rewriteEqualityToInPredicate
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode

trait QueryGraphProducer {
  self: LogicalPlanningTestSupport =>

  /**
   * Returns a planner query and semantic table for the given query.
   * The plan context defines one procedure:
   *
   * my.proc.foo(a: INT): (x: INT, y: NODE)
   */
  def producePlannerQueryForPattern(
    query: String,
    appendReturn: Boolean = true
  ): (SinglePlannerQuery, SemanticTable) = {
    val appendix = if (appendReturn) " RETURN 1 AS Result" else ""
    val q = query + appendix
    val exceptionFactory = Neo4jCypherExceptionFactory(q, None)
    val ast = parser.parse(q, exceptionFactory)
    val cleanedStatement: Statement =
      ast.endoRewrite(inSequence(normalizeWithAndReturnClauses(exceptionFactory)))
    val onError = SyntaxExceptionCreator.throwOnError(exceptionFactory)
    val SemanticCheckResult(semanticState, errors) =
      SemanticChecker.check(cleanedStatement, SemanticState.clean.withFeatures(semanticFeatures: _*))
    onError(errors)

    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))

    val signature =
      ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess, id = 42)
    val procLookup: QualifiedName => ProcedureSignature = _ => signature
    val fcnLookup: QualifiedName => Option[UserFunctionSignature] = _ => None
    val resolver = new TestSignatureResolvingPlanContext(procLookup, fcnLookup)

    val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator()
    // if you ever want to have parameters in here, fix the map
    val firstRewriteStep =
      ASTRewriter.rewrite(cleanedStatement, semanticState, Map.empty, exceptionFactory, anonymousVariableNameGenerator)
    val state = LogicalPlanState(
      query,
      None,
      IDPPlannerName,
      newStubbedPlanningAttributes,
      anonymousVariableNameGenerator,
      Some(resolver.procedureSignatureVersion),
      Some(firstRewriteStep),
      Some(semanticState)
    )
    val context = ContextHelper.create(
      logicalPlanIdGen = idGen,
      planContext = resolver
    )
    val output = (
      RewriteProcedureCalls andThen
        SemanticAnalysis(warn = false, semanticFeatures: _*) andThen
        Namespacer andThen
        rewriteEqualityToInPredicate andThen
        cnfNormalizerTransformer andThen
        collapseMultipleInPredicates
    ).transform(state, context)

    val semanticTable = output.semanticTable()
    val plannerQuery =
      StatementConverters.convertToPlannerQuery(
        output.statement().asInstanceOf[Query],
        semanticTable,
        anonymousVariableNameGenerator,
        CancellationChecker.NeverCancelled
      )
    (plannerQuery.asInstanceOf[SinglePlannerQuery], semanticTable)
  }
}
