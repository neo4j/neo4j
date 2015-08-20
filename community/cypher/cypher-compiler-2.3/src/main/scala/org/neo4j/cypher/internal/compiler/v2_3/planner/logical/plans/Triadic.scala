/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
Triadic build and probe is used to solve a common query pattern:
MATCH (a)-->(b)-->(c) WHERE NOT (a)-->(c)

If this query was solved by starting from (a) and expanding out, after expanding
to (b), a TriadicBuild would be done, which eagerly puts all seen (b) nodes in a Set
while expanding. After expanding to (c), we can check in the triadic set if the (c) is already seen.
 */
/*
 MATCH (<source>){-->(build)}{-->(target)}
 WHERE NOT (<source>)-->(<target>)         <=>  WHERE (<target>) NOT IN Set(<build>, for <source>)
 */
/* The Goal:

+Triadic
| \
| +<target>
| |
| +Argument
|
+<build>
|
+<source>
 */
case class Triadic(left: LogicalPlan /*produces rows with 'source' and 'seen'*/,
                   sourceId: IdName, seenId: IdName, targetId: IdName,
                   right: LogicalPlan /*given rows with 'source' and 'seen', produces rows with 'target'*/)
                  (val solved: PlannerQuery with CardinalityEstimation)
  extends LogicalPlan with LazyLogicalPlan with LogicalPlanWithoutExpressions {
  override def lhs: Option[LogicalPlan] = Some(left)

  override def rhs: Option[LogicalPlan] = Some(right)

  override def availableSymbols: Set[IdName] = left.availableSymbols ++ right.availableSymbols
}
