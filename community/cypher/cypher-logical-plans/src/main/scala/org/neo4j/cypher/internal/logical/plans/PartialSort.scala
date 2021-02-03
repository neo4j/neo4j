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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * Given an input that is sorted on a prefix of columns, e.g. [a],
 * produce an output that is sorted on more columns, e.g. [a, b, c].
 *
 * @param skipSortingPrefixLength skip sorting so many rows at the beginning.
 *                                This is an improvement if we know that these rows are skipped afterwards anyway.
 */
case class PartialSort(override val source: LogicalPlan,
                       alreadySortedPrefix: Seq[ColumnOrder],
                       stillToSortSuffix: Seq[ColumnOrder],
                       skipSortingPrefixLength: Option[Expression] = None)
                      (implicit idGen: IdGen) extends LogicalUnaryPlan(idGen)  {
  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)
  val availableSymbols: Set[String] = source.availableSymbols
}
