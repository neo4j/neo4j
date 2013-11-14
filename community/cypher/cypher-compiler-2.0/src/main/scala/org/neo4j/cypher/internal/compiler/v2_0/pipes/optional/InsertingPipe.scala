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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.optional

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.{QueryState, Pipe, PipeWithSource}
import symbols._

case class InsertingPipe(in: Pipe,
                         builder: Pipe => Pipe,
                         noMatch: (ExecutionContext, Seq[String], QueryState) => ExecutionContext)
  extends Pipe {
  val listenerPipe: ListenerPipe = new ListenerPipe(in)
  val innerPipe: Pipe = builder(listenerPipe)

  val addedIdentifiers: Seq[String] = {
    val identifiersAfterMatch: Set[String] = innerPipe.symbols.identifiers.map(_._1).toSet
    val identifiersBeforeMatch: Set[String] = in.symbols.identifiers.map(_._1).toSet

    (identifiersAfterMatch -- identifiersBeforeMatch).toSeq
  }

  def symbols: SymbolTable = innerPipe.symbols

  def executionPlanDescription: PlanDescription = {
    val innerPlanDescription = innerPipe.executionPlanDescription
    in.executionPlanDescription.andThenWrap(in, "NullableMatch", innerPlanDescription)
  }

  def throwIfSymbolsMissing(symbols: SymbolTable) {}

  case class ListenerPipe(source: Pipe) extends PipeWithSource(source) {
    def symbols: SymbolTable = source.symbols

    def executionPlanDescription: PlanDescription = source.executionPlanDescription

    def throwIfSymbolsMissing(symbols: SymbolTable) {}

    protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
      val listener = new Listener[ExecutionContext](input)
      state.listener = listener
      listener
    }
  }

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val innerResult = innerPipe.createResults(state)
    val listener = state.listener
    new InsertingIterator(listener, innerResult, noMatch(_: ExecutionContext, addedIdentifiers, state))
  }

  def exists(pred: Pipe => Boolean) = pred(this) || in.exists(pred) || innerPipe.exists(pred)
}
