/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3._

trait SymbolicName extends ASTNode with ASTParticle {
  def name: String
  def position: InputPosition
}

trait SymbolicNameWithId[+ID <: NameId] extends SymbolicName {
  def id(implicit semanticTable: SemanticTable): Option[ID]
}

case class LabelName(name: String)(val position: InputPosition) extends SymbolicNameWithId[LabelId] {
  def id(implicit semanticTable: SemanticTable): Option[LabelId] = semanticTable.resolvedLabelIds.get(name)
}

case class PropertyKeyName(name: String)(val position: InputPosition) extends SymbolicNameWithId[PropertyKeyId] {
  def id(implicit semanticTable: SemanticTable): Option[PropertyKeyId] = semanticTable.resolvedPropertyKeyNames.get(name)
}

case class RelTypeName(name: String)(val position: InputPosition) extends SymbolicNameWithId[RelTypeId] {
  def id(implicit semanticTable: SemanticTable): Option[RelTypeId] = semanticTable.resolvedRelTypeNames.get(name)
}
