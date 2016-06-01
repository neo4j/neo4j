/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.commands.predicates

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState

/*
This class is used for making the common <exp> IN <constant-expression> fast

It uses a cache for the <constant-expression> value, and turns it into a Set, for fast existence checking
 */
case class ConstantIn(value: Expression, list: Expression) extends Predicate with CollectionSupport {
  override def isMatch(ctx: ExecutionContext)(implicit state: QueryState) = {
    val setToCheck: Set[Any] = state.constantInCache.getOrElseUpdate(list, {
      val set = makeTraversable(list(ctx)).toSet
      set
    })

    val result = Option(value(ctx)) map setToCheck.apply

    // When checking if a value is contained in a collection that contains null, the result is null, and not false if
    // the value is not present in the collection
    if (result.contains(false) && setToCheck.contains(null))
      None
    else
      result
  }

  override def containsIsNull = false

  override def arguments = Seq(list)

  override def rewrite(f: (Expression) => Expression) = f(ConstantIn(value.rewrite(f), list.rewrite(f)))

  override def symbolTableDependencies = list.symbolTableDependencies
}
