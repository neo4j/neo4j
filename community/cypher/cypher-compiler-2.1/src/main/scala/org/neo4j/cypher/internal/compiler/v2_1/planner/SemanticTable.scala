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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, Expression}
import scala.collection.mutable

case class SemanticTable(types: IdentityMap[Expression, ExpressionTypeInfo] = IdentityMap.empty) {
  var resolvedLabelIds: mutable.Map[String, LabelId] = new mutable.HashMap[String, LabelId]
  var resolvedPropertyKeyNames: mutable.Map[String, PropertyKeyId] = new mutable.HashMap[String, PropertyKeyId]
  var resolvedRelTypeNames: mutable.Map[String, RelTypeId] = new mutable.HashMap[String, RelTypeId]

  def isRelationship(expr: Identifier) = types(expr).specified == symbols.CTRelationship.invariant

  def isNode(expr: Identifier) = types(expr).specified == symbols.CTNode.invariant
}
