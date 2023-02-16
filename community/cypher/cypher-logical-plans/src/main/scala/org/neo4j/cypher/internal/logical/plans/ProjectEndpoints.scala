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

import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.util.Preconditions

/**
 *
 *
 * This operator adheres to the following semantics. When we have a relationship list in scope and want retrieve the end
 * nodes of the resulting path like,
 *
 * {{{
 *   WITH ... AS rels
 *   MATCH (start)-[rels*]->(end)
 * }}}
 * 
 * it should be equivalent to
 *
 * {{{
 *   WITH ... AS rels
 *   MATCH (start)-[rels2*]->(end)
 *   WHERE rels = rels2
 * }}}
 *
 * ProjectEndpoints accepts some parameters who together describe a path:
 *  - a single relationship or a relationship list,
 *  - a list of relationship types,
 *  - the direction of the path,
 *  - length quantifiers if it's a var-length path,
 *  - the names of the start and end nodes, and whether they are in scope or not,
 * It then returns the start and end nodes of any paths which are induced by the relationship(s) and matches the pattern.
 * Just like in the example above. There are a couple of different cases that should be considered:
 *
 *  - If 'rels == NO_VALUE', or 'rels' doesn't induce a valid path, or 'rels' does not match the specified 'types',
 *    the operator will return nothing.
 *
 *  - If 'direction' is OUTGOING, then produce one row where start is set to the source node of the first relationship
 *   in rels, and end is set to the target node of the last relationship. If a given node is in scope, say start. Then
 *   we require that the node in scope is equal to the source node of the first relationship of the relationship list.
 *   Otherwise return nothing.
 *
 *   - If 'direction' is INCOMING, produce one row where start is set to the target node of the first relationship in
 *   the list, and end is set to the source node of the last relationship. If a given node is in scope, say start. Then
 *   we require that the node in scope is equal to the target node of the first relationship of the relationship list.
 *   Otherwise return nothing.
 *
 *   - if 'direction' is BOTH, produce rows for each path for which the relationship list is a valid path where
 *   the internal relationships may be oriented in any order. In most cases, this will only produce one row. I.e
 *   if for example,
 *
 *      rels = `[(0)-[0]->(1), (2)-[1]->(1)]`,
 *
 *   then there is only one way to orient the relationships to build a valid undirected path,
 *
 *      `(0)-[0]->(1)<-[1]-(2)`
 *
 *   and we would return one row where start = 0, end = 2. There are cases which will produce two rows. Consider
 *   for example a scenario where
 *
 *    rels = `[(0)-[0]->(1), (1)-[1]->(0)]`,
 *
 *   then there it's possible to create two valid undirected paths with the given relationship list,
 *
 *    `(0)-[0]->(1)-[1]->(0)`,
 *    `(1)<-[0]-(0)<-[1]-(1)`,
 *
 *   in which case we'd return two rows, one with start=end=0, and one with start=end=1.
 *
 *   ASSUMPTION:
 *   If neither the start or end node is in scope, then it's guaranteed that the relationship list is non-empty.
 *   Otherwise we'd essentially need to implement an all node scan inside projectEndpoints, as with nothing in scope
 *   the following
 *   {{{
 *    WITH [] AS rels
 *    MATCH (a)-[rels2*0..]->(b)
 *    WHERE rels = rels2
 *    return a, b
 *   }}},
 *   would return every single pair of nodes (a, a) in the graph.
 *
 */
case class ProjectEndpoints(
  override val source: LogicalPlan,
  rels: String,
  start: String,
  startInScope: Boolean,
  end: String,
  endInScope: Boolean,
  types: Seq[RelTypeName],
  direction: SemanticDirection,
  length: PatternLength
)(implicit idGen: IdGen)
    extends LogicalUnaryPlan(idGen) {

  Preconditions.checkArgument(
    startInScope || endInScope || length.isSimple || length.asInstanceOf[VarPatternLength].min > 0,
    "Var length pattern including length 0, with no end node in scope, must not be solved by ProjectEndpoints."
  )

  override def withLhs(newLHS: LogicalPlan)(idGen: IdGen): LogicalUnaryPlan = copy(source = newLHS)(idGen)

  val availableSymbols: Set[String] = source.availableSymbols + rels + start + end
}
