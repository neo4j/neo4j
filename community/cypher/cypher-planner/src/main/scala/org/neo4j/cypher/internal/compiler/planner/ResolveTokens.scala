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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.VisitorPhase
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
case object ResolveTokens extends VisitorPhase[PlannerContext, BaseState] with StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  def resolve(ast: Query)(implicit semanticTable: SemanticTable, tokenContext: ReadTokenContext): Unit = {
    ast.folder.fold(()) {
      case token: PropertyKeyName =>
        _ => resolvePropertyKeyName(token.name)
      case token: LabelName =>
        _ => resolveLabelName(token.name)
      case token: RelTypeName =>
        _ => resolveRelTypeName(token.name)
    }
  }

  private def resolvePropertyKeyName(name: String)(
    implicit semanticTable: SemanticTable,
    tokenContext: ReadTokenContext
  ): Unit = {
    tokenContext.getOptPropertyKeyId(name).map(PropertyKeyId) match {
      case Some(id) =>
        semanticTable.resolvedPropertyKeyNames += name -> id
      case None =>
    }
  }

  private def resolveLabelName(name: String)(
    implicit semanticTable: SemanticTable,
    tokenContext: ReadTokenContext
  ): Unit = {
    tokenContext.getOptLabelId(name).map(LabelId) match {
      case Some(id) =>
        semanticTable.resolvedLabelNames += name -> id
      case None =>
    }
  }

  private def resolveRelTypeName(name: String)(
    implicit semanticTable: SemanticTable,
    tokenContext: ReadTokenContext
  ): Unit = {
    tokenContext.getOptRelTypeId(name).map(RelTypeId) match {
      case Some(id) =>
        semanticTable.resolvedRelTypeNames += name -> id
      case None =>
    }
  }

  override def phase = AST_REWRITE

  override def visit(value: BaseState, context: PlannerContext): Unit = {
    value.statement() match {
      case q: Query => resolve(q)(value.semanticTable(), context.planContext)
      case _        =>
    }
    context.planContext.getPropertiesWithExistenceConstraint.foreach(resolvePropertyKeyName(_)(
      value.semanticTable(),
      context.planContext
    ))
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This sets fields in the semantic table
    BaseContains[SemanticTable]()
  )

  // necessary because VisitorPhase defines empty postConditions
  override def postConditions: Set[StepSequencer.Condition] = Set(completed)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): VisitorPhase[PlannerContext, BaseState] = this
}
