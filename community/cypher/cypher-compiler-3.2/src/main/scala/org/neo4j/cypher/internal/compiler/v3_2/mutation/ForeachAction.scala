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
package org.neo4j.cypher.internal.compiler.v3_2.mutation

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_2.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

case class ForeachAction(collection: Expression, id: String, actions: Seq[UpdateAction])
  extends UpdateAction
  with ListSupport {

  def exec(context: ExecutionContext, state: QueryState) = {
    val seq = makeTraversable(collection(context)(state))

    for (element <- seq) {
      val inner = context.newWith1(id, element)

      // We do a fold left here to allow updates to introduce
      // symbols in each others context.
      actions.foldLeft(Seq(inner))((contexts, action) => {
        contexts.flatMap(c => action.exec(c, state))
      })
    }

    Iterator(context)
  }

  override def updateSymbols(symbol: SymbolTable) = addInnerVariable(symbol)

  def children = collection +: actions

  def rewrite(f: (Expression) => Expression) = ForeachAction(f(collection), id, actions.map(_.rewrite(f)))

  def variables = Nil

  def addInnerVariable(symbols: SymbolTable): SymbolTable = {
    val t = collection.evaluateType(CTList(CTAny), symbols).legacyIteratedType

    val innerSymbols: SymbolTable = symbols.add(id, t)
    innerSymbols
  }

  def symbolTableDependencies = {
    val updateActionsDeps: Set[String] = actions.flatMap(_.symbolTableDependencies).toSet
    val updateActionVariables: Set[String] = actions.flatMap(_.variables.map(_._1)).toSet
    val collectionDeps = collection.symbolTableDependencies

    (updateActionsDeps -- updateActionVariables) ++ collectionDeps -- Some(id)
  }
}
