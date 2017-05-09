/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_3.symbols._

case class ReduceFunction(collection: Expression, id: String, expression: Expression, acc: String, init: Expression)
  extends NullInNullOutExpression(collection) with ListSupport {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = {
    val initMap = m.newWith(acc -> init(m))
    val computedMap = makeTraversable(value).foldLeft(initMap) { (accMap, k) => {
        val innerMap = accMap.newWith(id -> k)
        innerMap.newWith(acc -> expression(innerMap))
      }
    }
    computedMap(acc)
  }

  def rewrite(f: (Expression) => Expression) =
    f(ReduceFunction(collection.rewrite(f), id, expression.rewrite(f), acc, init.rewrite(f)))

  def arguments: Seq[Expression] = Seq(collection, init)

  override def children = Seq(collection, expression, init)

  def variableDependencies(expectedType: CypherType) = AnyType

  def symbolTableDependencies = (collection.symbolTableDependencies ++ expression.symbolTableDependencies ++ init.symbolTableDependencies) - id - acc
}
