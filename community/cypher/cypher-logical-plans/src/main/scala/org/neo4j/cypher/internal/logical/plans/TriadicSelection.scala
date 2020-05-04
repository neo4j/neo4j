/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.util.attribution.IdGen

/**
 * Triadic selection is used to solve a common query pattern:
 * MATCH (a)-->(b)-->(c) WHERE NOT (a)-->(c)
 *
 * If this query can be solved by starting from (a) and expand first (a)-->(b)
 * and expanding (b)-->(c), we can replace the filter with a triadic selection
 * that runs the (a)-->(b) as its left hand side, caching the results for use in
 * filtering the results of its right hand side which is the (b)-->(c) expands.
 * The filtering is based on the pattern expression predicate. The classical
 * example is the friend of a friend that is not already a friend, as shown above,
 * but this works for other cases too, like fof that is a friend.
 *
 * Since the two expands are done by sub-plans, they can be substantially more
 * complex than single expands. However, what patterns actually get here need to
 * be identified by the triadic selection finder.
 *
 * In effect the triadic selection interprets the predicate pattern in:
 *     MATCH (<source>){-->(build)}{-->(target)}
 *     WHERE NOT (<source>)-->(<target>)
 *
 * as the predicate:
 *
 * WHERE (<target>) NOT IN Set(<build>, for <source>)
 *
 * With a plan that looks like:
 *
 * +TriadicSelection (c) NOT IN (b)
 * | \
 * | +<target>       (b)-->(c)
 * | |
 * | +Argument       (b)
 * |
 * +<build>          (a)-->(b)
 * |
 * +<source>         (a)
 */
case class TriadicSelection(left: LogicalPlan,
                            right: LogicalPlan,
                            positivePredicate: Boolean,
                            sourceId: String,
                            seenId: String,
                            targetId: String
                           )(implicit idGen: IdGen)
  extends LogicalPlan(idGen) with ApplyPlan {

  override def lhs: Option[LogicalPlan] = Some(left)

  override def rhs: Option[LogicalPlan] = Some(right)

  override val availableSymbols: Set[String] = left.availableSymbols ++ right.availableSymbols
}
