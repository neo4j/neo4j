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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.ListSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues

case class FilterFunction(collection: Expression, id: String, predicate: Predicate)
  extends NullInNullOutExpression(collection)
  with ListSupport
  with Closure {

  def compute(value: AnyValue, m: ExecutionContext)(implicit state: QueryState) = {
    val traversable = makeTraversable(value)
    VirtualValues.filter(traversable, new java.util.function.Function[AnyValue, java.lang.Boolean]() {
      override def apply(v1: AnyValue): java.lang.Boolean =  predicate.isTrue(m.newWith1(id, v1)  )
    })
  }

  def rewrite(f: (Expression) => Expression) =
    f(FilterFunction(collection.rewrite(f), id, predicate.rewriteAsPredicate(f)))

  override def children = Seq(collection, predicate)

  def arguments: Seq[Expression] = Seq(collection)

  def symbolTableDependencies = symbolTableDependencies(collection, predicate, id)
}
