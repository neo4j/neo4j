/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.symbols.{CypherType, _}

sealed trait Command extends Statement {
  override def returnColumns = List.empty
}

case class CreateIndex(label: LabelName, property: PropertyKeyName)(val position: InputPosition) extends Command {
  def semanticCheck = Seq()
}

case class DropIndex(label: LabelName, property: PropertyKeyName)(val position: InputPosition) extends Command {
  def semanticCheck = Seq()
}

trait PropertyConstraintCommand extends Command with SemanticChecking {
  def variable: Variable

  def property: Property

  def entityType: CypherType

  def semanticCheck =
    variable.declare(entityType) chain
      property.semanticCheck(Expression.SemanticContext.Simple) chain
      when(!property.map.isInstanceOf[ast.Variable]) {
        SemanticError("Cannot index nested properties", property.position)
      }
}

trait NodePropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType = symbols.CTNode

  def label: LabelName
}

trait RelationshipPropertyConstraintCommand extends PropertyConstraintCommand {

  val entityType = symbols.CTRelationship

  def relType: RelTypeName
}

case class CreateUniquePropertyConstraint(variable: Variable, label: LabelName, property: Property)(val position: InputPosition) extends NodePropertyConstraintCommand

case class DropUniquePropertyConstraint(variable: Variable, label: LabelName, property: Property)(val position: InputPosition) extends NodePropertyConstraintCommand

case class CreateNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property)(val position: InputPosition) extends NodePropertyConstraintCommand

case class DropNodePropertyExistenceConstraint(variable: Variable, label: LabelName, property: Property)(val position: InputPosition) extends NodePropertyConstraintCommand

case class CreateRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property)(val position: InputPosition) extends RelationshipPropertyConstraintCommand

case class DropRelationshipPropertyExistenceConstraint(variable: Variable, relType: RelTypeName, property: Property)(val position: InputPosition) extends RelationshipPropertyConstraintCommand
