/**
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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, InputPosition}
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticTable

trait SymbolicName {
  self: ASTNode =>

  def name: String
  def position: InputPosition
}

final case class LabelName(name: String)(val position: InputPosition) extends ASTNode with SymbolicName

object LabelName {
  implicit class LabelNameId(that: LabelName)(implicit semanticTable: SemanticTable) {
    def id = semanticTable.resolvedLabelIds.get(that.name)

    def either = that.id match {
      case Some(id) => Right(id)
      case None => Left(that.name)
    }
  }
}

final case class PropertyKeyName(name: String)(val position: InputPosition) extends ASTNode with SymbolicName

object PropertyKeyName {
  implicit class PropertyKeyNameId(that: PropertyKeyName)(implicit semanticTable: SemanticTable) {
    def id: Option[PropertyKeyId] = semanticTable.resolvedPropertyKeyNames.get(that.name)

    def either = that.id match {
      case Some(id) => Right(id)
      case None => Left(that.name)
    }
  }
}

final case class RelTypeName(name: String)(val position: InputPosition) extends ASTNode with SymbolicName

object RelTypeName {
  implicit class RelTypeNameId(that: RelTypeName)(implicit semanticTable: SemanticTable) {
    def id = semanticTable.resolvedRelTypeNames.get(that.name)

    def either = that.id match {
      case Some(id) => Right(id)
      case None => Left(that.name)
    }
  }
}
