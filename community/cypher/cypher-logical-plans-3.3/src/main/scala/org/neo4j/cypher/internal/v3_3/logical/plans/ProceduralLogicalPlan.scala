/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.v3_3.logical.plans

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.symbols.CypherType
import org.neo4j.cypher.internal.ir.v3_4._

abstract class ProceduralLogicalPlan extends LogicalPlan {
  override def lhs: Option[LogicalPlan] = None

  override def rhs: Option[LogicalPlan] = None

  override def solved: PlannerQuery with CardinalityEstimation = CardinalityEstimation.lift(PlannerQuery.empty, 1.0)

  override def availableSymbols: Set[IdName] = Set.empty

  override def strictness: StrictnessMode = LazyMode

}

case class StandAloneProcedureCall(signature: ProcedureSignature,
                                   args: Seq[Expression],
                                   types: Seq[(String, CypherType)],
                                   callResultIndices: Seq[(Int, String)]) extends ProceduralLogicalPlan

case class CreateNodeKeyConstraint(node: IdName, label: LabelName, props: Seq[Property]) extends ProceduralLogicalPlan
case class DropNodeKeyConstraint(label: LabelName, props: Seq[Property]) extends ProceduralLogicalPlan

case class CreateUniquePropertyConstraint(node: IdName, label: LabelName, props: Seq[Property]) extends ProceduralLogicalPlan
case class DropUniquePropertyConstraint(label: LabelName, props: Seq[Property]) extends ProceduralLogicalPlan

case class CreateNodePropertyExistenceConstraint(label: LabelName, prop: Property) extends ProceduralLogicalPlan
case class DropNodePropertyExistenceConstraint(label: LabelName, prop: Property) extends ProceduralLogicalPlan

case class CreateRelationshipPropertyExistenceConstraint(typeName: RelTypeName, prop: Property) extends ProceduralLogicalPlan
case class DropRelationshipPropertyExistenceConstraint(typeName: RelTypeName, prop: Property) extends ProceduralLogicalPlan

case class CreateIndex(label: LabelName, propertyKeyNames: List[PropertyKeyName]) extends ProceduralLogicalPlan
case class DropIndex(label: LabelName, propertyKeyNames: List[PropertyKeyName]) extends ProceduralLogicalPlan
