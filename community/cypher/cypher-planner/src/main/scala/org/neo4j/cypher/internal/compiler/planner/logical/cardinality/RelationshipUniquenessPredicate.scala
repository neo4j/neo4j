/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.RelationshipUniquenessPredicate.areRelationships
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Variable

object RelationshipUniquenessPredicate {

  def unapply(arg: Any): Option[RelationshipUniquenessPredicate] = arg match {
    case Not(Equals(lhs: Variable, rhs: Variable)) => Some(RelationshipUniquenessPredicate(lhs, rhs))
    case _                                         => None
  }

  private def areRelationships(semanticTable: SemanticTable, lhs: Variable, rhs: Variable): Boolean = {
    val l = semanticTable.isRelationshipNoFail(lhs)
    val r = semanticTable.isRelationshipNoFail(rhs)
    l && r
  }
}

case class RelationshipUniquenessPredicate(lhs: Variable, rhs: Variable) {
  def isOnRelationships(semanticTable: SemanticTable): Boolean = areRelationships(semanticTable, lhs, rhs)
}
