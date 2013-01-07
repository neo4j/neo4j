/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.helpers.CollectionSupport
import org.neo4j.cypher.internal.symbols.{AnyCollectionType, SymbolTable}
import org.neo4j.cypher.internal.pipes.{QueryState}
import org.neo4j.cypher.internal.ExecutionContext

case class ForeachAction(collection: Expression, id: String, actions: Seq[UpdateAction])
  extends UpdateAction
  with CollectionSupport {
  def exec(context: ExecutionContext, state: QueryState) = {
    val before = context.get(id)

    val seq = makeTraversable(collection(context))
    seq.foreach(element => {
      context.put(id, element)

      // We do a fold left here to allow updates to introduce
      // symbols in each others context.
      actions.foldLeft(Seq(context))((contexts, action) => {
        contexts.flatMap(c => action.exec(c, state))
      })
    })

    before match {
      case None      => context.remove(id)
      case Some(old) => context.put(id, old)
    }

    Stream(context)
  }

  def children = Seq(collection) ++ actions.flatMap(_.children)

  def rewrite(f: (Expression) => Expression) = ForeachAction(f(collection), id, actions.map(_.rewrite(f)))

  def identifiers = Seq.empty

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    val t = collection.evaluateType(AnyCollectionType(), symbols).iteratedType

    val innerSymbols: SymbolTable = symbols.add(id, t)

    actions.foreach(_.throwIfSymbolsMissing(innerSymbols))
  }

  def symbolTableDependencies = {
    val updateActionsDeps: Set[String] = actions.flatMap(_.symbolTableDependencies).toSet
    val updateActionIdentifiers: Set[String] = actions.flatMap(_.identifiers.map(_._1)).toSet
    val collectionDeps = collection.symbolTableDependencies

    (updateActionsDeps -- updateActionIdentifiers) ++ collectionDeps -- Some(id)
  }
}