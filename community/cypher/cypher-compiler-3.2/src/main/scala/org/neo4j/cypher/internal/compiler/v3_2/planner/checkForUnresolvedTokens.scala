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
package org.neo4j.cypher.internal.compiler.v3_2.planner

import org.neo4j.cypher.internal.frontend.v3_2.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.notification.{MissingPropertyNameNotification, MissingRelTypeNotification, MissingLabelNotification, InternalNotification}

/**
  * Parses ast and looks for unresolved tokens
  */
object checkForUnresolvedTokens extends ((Query, SemanticTable) => Seq[InternalNotification]) {

  def apply(ast: Query, table: SemanticTable) = {
    def isEmptyLabel(label: String) = !table.resolvedLabelIds.contains(label)
    def isEmptyRelType(relType: String) = !table.resolvedRelTypeNames.contains(relType)
    def isEmptyPropertyName(name: String) = !table.resolvedPropertyKeyNames.contains(name)

    ast.treeFold(Seq.empty[InternalNotification]) {
      case label@LabelName(name) if isEmptyLabel(name) => acc =>
        (acc :+ MissingLabelNotification(label.position, name), Some(identity))

      case rel@RelTypeName(name) if isEmptyRelType(name) => acc =>
        (acc :+ MissingRelTypeNotification(rel.position, name), Some(identity))

      case Property(_, prop@PropertyKeyName(name)) if isEmptyPropertyName(name) => acc =>
        (acc :+ MissingPropertyNameNotification(prop.position, name), Some(identity))
    }
  }
}
