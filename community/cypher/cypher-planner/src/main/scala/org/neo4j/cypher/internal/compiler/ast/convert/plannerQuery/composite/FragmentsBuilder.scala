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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite.CompositeQuery.Fragment
import org.neo4j.cypher.internal.expressions.LogicalVariable

import scala.collection.mutable.ListBuffer

/**
 * Mutable builder for [[CompositeQuery.Single.Fragments]].
 * Groups consecutive clauses together interspersed by sub-queries.
 * For example:
 *
 * {{{
 *   MATCH (a:A)
 *   OPTIONAL MATCH (a)-[r:R]->(b:B)
 *   WITH a.prop AS x
 *   CALL { sub-query 1 }
 *   CREATE (:X {prop: x})
 *   CALL { sub-query 2 }
 *   WITH * SKIP 1
 *   RETURN x
 * }}}
 *
 * gets fragmented as:
 *
 * {{{
 *   [
 *     [MATCH (a:A), OPTIONAL MATCH (a)-[r:R]->(b:B), WITH a.prop AS x],
 *     sub-query 1,
 *     [CREATE (:X {prop: x})],
 *     sub-query 2,
 *     [WITH * SKIP 1, RETURN x]
 *   ]
 * }}}
 *
 * @param arguments the return items of the importing with of the single query being fragmented
 */
class FragmentsBuilder(arguments: Set[LogicalVariable]) {
  private val fragments = Seq.newBuilder[CompositeQuery.Fragment]
  private val clauses = ListBuffer.empty[ast.Clause]

  def addStandardClause(clause: ast.Clause): Unit =
    clauses.addOne(clause)

  def addSubQuery(
    innerQuery: CompositeQuery,
    isCorrelated: Boolean,
    isYielding: Boolean,
    inTransactionsParameters: Option[ast.SubqueryCall.InTransactionsParameters]
  ): Unit = {
    commitLastClausesToNewFragment()
    val subQueryFragment = Fragment.SubQuery(innerQuery, isCorrelated, isYielding, inTransactionsParameters)
    fragments.addOne(subQueryFragment)
  }

  def result(): CompositeQuery.Single.Fragments = {
    commitLastClausesToNewFragment()
    val finalFragments = fragments.result()
    CompositeQuery.Single.Fragments(arguments, finalFragments)
  }

  private def commitLastClausesToNewFragment(): Unit =
    if (clauses.nonEmpty) { // if the last elements added to the builder were clauses:
      val standardFragment = Fragment.Standard(clauses.result()) // package them into a new fragment,
      fragments.addOne(standardFragment) // add it to the fragments builder,
      clauses.clear() // and reset the clauses builder.
    }
}
