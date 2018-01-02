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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery}


/*
Triadic selection is used to solve a common query pattern:
MATCH (a)-->(b)-->(c) WHERE NOT (a)-->(c)

If this query can be solved by starting from (a) and expand first (a)-->(b)
and expanding (b)-->(c), we can repalce the filter with a triadic selection
that runs the (a)-->(b) as its left hand side, caching the results for use in
filtering the results of its right hand side which is the (b)-->(c) expands.
The filtering is based on the pattern expression predicate. The classical
example is the friend of a friend that is not already a friend, as shown above,
but this works for other cases too, like fof that is a friend.

Since the two expands are done by sub-plans, they can be substantially more
complex than single expands. However, what patterns actually get here need to
be identified by the triadic selection finder.

In effect the triadic selection interprets the predicate pattern in:
    MATCH (<source>){-->(build)}{-->(target)}
    WHERE NOT (<source>)-->(<target>)

as the predicate:

    WHERE (<target>) NOT IN Set(<build>, for <source>)

With a plan that looks like:

+TriadicSelection (c) NOT IN (b)
| \
| +<target>       (b)-->(c)
| |
| +Argument       (b)
|
+<build>          (a)-->(b)
|
+<source>         (a)
 */
case class TriadicSelection(positivePredicate: Boolean /*false means NOT(pattern)*/ ,
                            left: LogicalPlan /*produces rows with 'source' and 'seen'*/ ,
                            sourceId: IdName, seenId: IdName, targetId: IdName,
                            right: LogicalPlan /*given rows with 'source' and 'seen', produces rows with 'target'*/)
                           (val solved: PlannerQuery with CardinalityEstimation)
extends LogicalPlan with LazyLogicalPlan with LogicalPlanWithoutExpressions {
  override def lhs: Option[LogicalPlan] = Some(left)

  override def rhs: Option[LogicalPlan] = Some(right)

  override def availableSymbols: Set[IdName] = left.availableSymbols ++ right.availableSymbols
}
