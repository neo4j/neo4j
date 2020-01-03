/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.compiler.phases._
import org.neo4j.cypher.internal.v4_0.ast.Query
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.v4_0.frontend.phases.{BaseState, VisitorPhase}
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId, RelTypeId}
import org.neo4j.cypher.internal.v4_0.expressions.{LabelName, PropertyKeyName, RelTypeName}

object ResolveTokens extends VisitorPhase[PlannerContext, BaseState] {
  def resolve(ast: Query)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    ast.fold(()) {
      case token: PropertyKeyName =>
        _ => resolvePropertyKeyName(token.name)
      case token: LabelName =>
        _ => resolveLabelName(token.name)
      case token: RelTypeName =>
        _ => resolveRelTypeName(token.name)
    }
  }

  private def resolvePropertyKeyName(name: String)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    tokenContext.getOptPropertyKeyId(name).map(PropertyKeyId) match {
      case Some(id) =>
        semanticTable.resolvedPropertyKeyNames += name -> id
      case None =>
    }
  }

  private def resolveLabelName(name: String)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    tokenContext.getOptLabelId(name).map(LabelId) match {
      case Some(id) =>
        semanticTable.resolvedLabelNames += name -> id
      case None =>
    }
  }

  private def resolveRelTypeName(name: String)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    tokenContext.getOptRelTypeId(name).map(RelTypeId) match {
      case Some(id) =>
        semanticTable.resolvedRelTypeNames += name -> id
      case None =>
    }
  }

  override def phase = AST_REWRITE

  override def description = "resolve token ids for labels, property keys and relationship types"

  override def visit(value: BaseState, context: PlannerContext): Unit = value.statement() match {
    case q: Query => resolve(q)(value.semanticTable(), context.planContext)
    case _ =>
  }
}
