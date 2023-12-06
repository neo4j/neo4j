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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition

/**
 * Resolve token ids for labels, property keys and relationship types.
 */
case object ResolveTokens extends Phase[PlannerContext, BaseState, BaseState] with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  private[planner] def resolve(ast: Query, semanticTable: SemanticTable)(
    implicit tokenContext: ReadTokenContext
  ): SemanticTable = {
    ast.folder.fold(semanticTable) {
      case token: PropertyKeyName =>
        acc => resolvePropertyKeyName(token.name, acc)
      case token: LabelName =>
        acc => resolveLabelName(token.name, acc)
      case token: RelTypeName =>
        acc => resolveRelTypeName(token.name, acc)
    }
  }

  private def resolvePropertyKeyName(name: String, semanticTable: SemanticTable)(
    implicit tokenContext: ReadTokenContext
  ): SemanticTable = {
    tokenContext.getOptPropertyKeyId(name).map(PropertyKeyId) match {
      case Some(id) =>
        semanticTable.addResolvedPropertyKeyName(name, id)
      case None => semanticTable
    }
  }

  private def resolveLabelName(name: String, semanticTable: SemanticTable)(
    implicit tokenContext: ReadTokenContext
  ): SemanticTable = {
    tokenContext.getOptLabelId(name).map(LabelId) match {
      case Some(id) =>
        semanticTable.addResolvedLabelName(name, id)
      case None => semanticTable
    }
  }

  private def resolveRelTypeName(name: String, semanticTable: SemanticTable)(
    implicit tokenContext: ReadTokenContext
  ): SemanticTable = {
    tokenContext.getOptRelTypeId(name).map(RelTypeId) match {
      case Some(id) =>
        semanticTable.addResolvedRelTypeName(name, id)
      case None => semanticTable
    }
  }

  override def phase = AST_REWRITE

  override def process(from: BaseState, context: PlannerContext): BaseState = {
    val semTab1 = from.statement() match {
      case q: Query => resolve(q, from.semanticTable())(context.planContext)
      case _        => from.semanticTable()
    }
    val result = context.planContext.getPropertiesWithExistenceConstraint.foldLeft(semTab1) {
      case (acc, name) => resolvePropertyKeyName(name, acc)(context.planContext)
    }

    from.withSemanticTable(result)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This sets fields in the semantic table
    BaseContains[SemanticTable](),
    BaseContains[Statement]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): ResolveTokens.type = this
}
