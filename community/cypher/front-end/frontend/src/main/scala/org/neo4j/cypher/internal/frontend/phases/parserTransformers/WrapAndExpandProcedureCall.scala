/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.AddedInRewriteProcCall
import org.neo4j.cypher.internal.ast.AdditiveProjection
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.Optional
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnAddedInRewrite
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.RewrittenOptional
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoExpandableClauses
import org.neo4j.cypher.internal.rewriting.conditions.ProcedureCallWrappedAndExpanded
import org.neo4j.cypher.internal.rewriting.conditions.ProjectionClausesHaveSemanticInfo
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites CALL proc WHERE <p> ==> CALL proc WITH * WHERE <p>
 * Rewrites OPTIONAL CALL neo4j.proc(a,b) YIELD x, y to OPTIONAL CALL (*) { CALL neo4j.proc(a,b) YIELD x, y RETURN x, y }
 * Standalone procedure calls are not wrapped, e.g. OPTIONAL CALL neo4j.proc
 */
case object WrapAndExpandProcedureCall extends StatementRewriter with ParsePipelineTransformerFactory with Step {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(ProcedureCallWrappedAndExpanded)

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    Set(ContainsNoExpandableClauses, ProjectionClausesHaveSemanticInfo, UpToDateScopes)

  private def expandWhere(call: UnresolvedCall): Seq[Clause] = call match {
    case unresolved @ UnresolvedCall(_, _, Some(result @ ProcedureResult(_, optWhere @ Some(where))), _, _, _) =>
      val newResult = result.copy(where = None)(result.position)
      val newUnresolved = unresolved.copy(declaredResult = Some(newResult))(unresolved.position)
      val newItems = ReturnItems(AdditiveProjection, Seq.empty)(where.position)
      val newWith =
        With(distinct = false, newItems, None, None, None, None, optWhere, AddedInRewriteProcCall)(where.position)
      Seq(newUnresolved, newWith)
    case call =>
      Seq(call)
  }

  private val rewriter: Rewriter = bottomUp(Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      val newClauses = clauses.flatMap {
        case unresolved @ UnresolvedCall(_, _, Some(_), _, _, Optional) =>
          val pos = unresolved.position
          val expandedCall = expandWhere(unresolved.copy(optionalState = RewrittenOptional)(pos))
          val returnItems = unresolved.returnVariables.explicitVariables.map(x => AliasedReturnItem(x))
          val returnClause =
            if (returnItems.nonEmpty)
              Seq(Return(ReturnItems(FreeProjection, returnItems)(pos), ReturnAddedInRewrite)(pos))
            else Seq.empty
          val innerQuery = SingleQuery(expandedCall ++ returnClause)(pos)
          Seq(ScopeClauseSubqueryCall(
            innerQuery,
            isImportingAll = true,
            Seq.empty,
            None,
            optional = true,
            addedInRewriteOptionalCall = true
          )(pos))
        case unresolved: UnresolvedCall =>
          expandWhere(unresolved)
        case x => Seq(x)
      }
      query.copy(newClauses)(query.position)
  })

  override def instance(from: BaseState, context: BaseContext): Rewriter = rewriter

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this
}
