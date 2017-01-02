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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.compiler.v2_2._
import symbols._

sealed trait Command extends Statement

case class CreateIndex(label: LabelName, property: PropertyKeyName)(val position: InputPosition) extends Command {
  def semanticCheck = Seq()
}

case class DropIndex(label: LabelName, property: PropertyKeyName)(val position: InputPosition) extends Command {
  def semanticCheck = Seq()
}

trait UniqueConstraintCommand extends Command with SemanticChecking {
  def identifier: Identifier
  def label: LabelName
  def property: Property

  def semanticCheck =
    identifier.declare(CTNode.covariant) chain
    property.semanticCheck(Expression.SemanticContext.Simple) chain
    when (!property.map.isInstanceOf[ast.Identifier]) {
      SemanticError("Cannot index nested properties", property.position)
    }
}

case class CreateUniqueConstraint(identifier: Identifier, label: LabelName, property: Property)(val position: InputPosition) extends UniqueConstraintCommand

case class DropUniqueConstraint(identifier: Identifier, label: LabelName, property: Property)(val position: InputPosition) extends UniqueConstraintCommand
