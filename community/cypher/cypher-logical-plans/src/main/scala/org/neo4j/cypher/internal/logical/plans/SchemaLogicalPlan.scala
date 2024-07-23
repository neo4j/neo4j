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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.ast.CreateConstraintType
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.expressions.ElementTypeName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.graphdb.schema.IndexType

abstract class SchemaLogicalPlan(idGen: IdGen) extends LogicalPlanExtension(idGen) {
  override def lhs: Option[LogicalPlan] = None

  override def rhs: Option[LogicalPlan] = None

  override val availableSymbols: Set[LogicalVariable] = Set.empty
}

case class CreateConstraint(
  source: Option[DoNothingIfExistsForConstraint],
  constraintType: CreateConstraintType,
  entityName: ElementTypeName,
  props: Seq[Property],
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source
}

case class DropConstraintOnName(
  name: Either[String, Parameter],
  ifExists: Boolean
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen)

case class CreateIndex(
  source: Option[DoNothingIfExistsForIndex],
  indexType: IndexType,
  entityName: ElementTypeName,
  propertyKeyNames: List[PropertyKeyName],
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source
}

case class CreateLookupIndex(
  source: Option[DoNothingIfExistsForLookupIndex],
  entityType: EntityType,
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source
}

case class CreateFulltextIndex(
  source: Option[DoNothingIfExistsForFulltextIndex],
  entityNames: Either[List[LabelName], List[RelTypeName]],
  propertyKeyNames: List[PropertyKeyName],
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen) {
  override def lhs: Option[LogicalPlan] = source
}

case class DropIndexOnName(
  name: Either[String, Parameter],
  ifExists: Boolean
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen)

case class DoNothingIfExistsForIndex(
  entityName: ElementTypeName,
  propertyKeyNames: List[PropertyKeyName],
  indexType: IndexType,
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen)

case class DoNothingIfExistsForLookupIndex(
  entityType: EntityType,
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen)
    extends SchemaLogicalPlan(idGen)

case class DoNothingIfExistsForFulltextIndex(
  entityNames: Either[List[LabelName], List[RelTypeName]],
  propertyKeyNames: List[PropertyKeyName],
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen)

case class DoNothingIfExistsForConstraint(
  entityName: ElementTypeName,
  props: Seq[Property],
  assertion: CreateConstraintType,
  name: Option[Either[String, Parameter]],
  options: Options
)(implicit idGen: IdGen) extends SchemaLogicalPlan(idGen)
