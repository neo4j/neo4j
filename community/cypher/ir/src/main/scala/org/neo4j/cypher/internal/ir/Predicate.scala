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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable

case class Predicate(dependencies: Set[LogicalVariable], expr: Expression) {

  def hasDependenciesMet(symbols: Set[LogicalVariable]): Boolean = dependencies.forall(symbols.contains)

  def hasDependenciesMetForRequiredSymbol(symbols: Set[LogicalVariable], required: LogicalVariable): Boolean =
    dependencies.contains(required) && hasDependenciesMet(symbols)
}

object Predicate {
  implicit val byPosition: Ordering[Predicate] = Ordering.by { (predicate: Predicate) => predicate.expr.position }
}
