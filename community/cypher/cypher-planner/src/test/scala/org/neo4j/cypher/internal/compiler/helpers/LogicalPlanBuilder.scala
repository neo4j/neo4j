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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.ast.semantics.{ExpressionTypeInfo, SemanticTable}
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Variable}
import org.neo4j.cypher.internal.v4_0.util.Cardinality
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.cypher.internal.v4_0.util.symbols.{CTNode, CypherType}

class LogicalPlanBuilder extends AbstractLogicalPlanBuilder[LogicalPlan, LogicalPlanBuilder](new LogicalPlanResolver) {

  class CardinalitiesWithDefault extends Cardinalities {
    override def get(id: Id): Cardinality =
      if (isDefinedAt(id)) super.get(id) else Cardinality.SINGLE
  }

  val cardinalities: Cardinalities = new CardinalitiesWithDefault

  private var semanticTable = new SemanticTable()

  override def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  override def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  def getSemanticTable: SemanticTable = semanticTable

  def withCardinality(x: Int): LogicalPlanBuilder = {
    cardinalities.set(idOfLastPlan, Cardinality(x))
    this
  }

  def newVar(name: String, typ: CypherType): LogicalPlanBuilder = {
    val variable: Expression = varFor(name)
    semanticTable = semanticTable.copy(types = semanticTable.types.updated(variable, ExpressionTypeInfo(typ.invariant, None)))
    this
  }

  def build(readOnly: Boolean = true): LogicalPlan = {
    buildLogicalPlan()
  }
}
