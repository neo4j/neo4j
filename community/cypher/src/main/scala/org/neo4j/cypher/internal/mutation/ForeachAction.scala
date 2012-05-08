/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.mutation

import org.neo4j.cypher.internal.commands.{IterableSupport, Expression}
import org.neo4j.cypher.internal.symbols.AnyIterableType
import org.neo4j.cypher.internal.pipes.{QueryState, ExecutionContext}


case class ForeachAction(iterable: Expression, symbol: String, actions: Seq[UpdateAction])
  extends UpdateAction
  with IterableSupport {
  def dependencies = {
    val ownIdentifiers = actions.flatMap(_.identifier)

    val updateDeps = actions.flatMap(_.dependencies).
      filterNot(_.name == symbol). //remove dependencies to the symbol we're introducing
      filterNot(ownIdentifiers contains) //remove dependencies to identifiers we are introducing

    iterable.dependencies(AnyIterableType()) ++ updateDeps
  }

  def exec(context: ExecutionContext, state: QueryState) = {
    val before = context.get(symbol)

    val seq = makeTraversable(iterable(context))
    seq.foreach(element => {
      context.put(symbol, element)

      // We do a fold left here to allow updates to introduce
      // symbols in each others context.
      actions.foldLeft(Seq(context))((contexts, action) => {
        contexts.flatMap(c => action.exec(c, state))
      })
    })

    before match {
      case None => context.remove(symbol)
      case Some(old) => context.put(symbol, old)
    }

    Stream(context)
  }

  def filter(f: (Expression) => Boolean) = Some(iterable).filter(f).toSeq ++ actions.flatMap(_.filter(f))

  def rewrite(f: (Expression) => Expression) = ForeachAction(f(iterable), symbol, actions.map(_.rewrite(f)))

  def identifier = Seq.empty
}