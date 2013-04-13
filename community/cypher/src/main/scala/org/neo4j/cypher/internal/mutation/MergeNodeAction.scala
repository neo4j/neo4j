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

import org.neo4j.cypher.internal.symbols.{NodeType, CypherType, SymbolTable}
import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.{EntityProducer, QueryState}
import org.neo4j.cypher.internal.commands.Predicate
import org.neo4j.cypher.{InternalException, CypherTypeException}
import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.spi.QueryContext


case class MergeNodeAction(identifier: String,
                           expectations: Seq[Predicate],
                           onCreate: Seq[UpdateAction],
                           onUpdate: Seq[UpdateAction],
                           nodeProducerOption: Option[EntityProducer[Node]]) extends UpdateAction {
  def children = expectations ++ onCreate ++ onUpdate

  lazy val nodeProducer: EntityProducer[Node] = nodeProducerOption.getOrElse(throw new InternalException(
    "Tried to run merge action without finding node producer. This should never happen. " +
      "It seems the execution plan builder failed. "))

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = {
    val foundNodes: Iterator[Node] = nodeProducer(context, state)

    if (foundNodes.isEmpty) {
      val query: QueryContext = state.query
      val createdNode: Node = query.createNode()
      val newContext = context += (identifier -> createdNode)

      onCreate.foreach {
        action => action.exec(newContext, state)
      }

      Iterator(newContext)
    } else {
      foundNodes.map(n => context.newWith(identifier -> n))
    }
  }

  def identifiers: Seq[(String, CypherType)] = Seq(identifier -> NodeType())

  def rewrite(f: (Expression) => Expression) =
    MergeNodeAction(identifier = identifier,
      expectations = expectations.map(_.rewrite(f)),
      onCreate = onCreate.map(_.rewrite(f)),
      onUpdate = onCreate.map(_.rewrite(f)),
      nodeProducerOption)

  def throwIfSymbolsMissing(in: SymbolTable) {

    if (in.keys.contains(identifier))
      throw new CypherTypeException(identifier + " already defined.")

    val symbols = in.add(identifier, NodeType())

    expectations.foreach(_.throwIfSymbolsMissing(symbols))
    onCreate.foreach(_.throwIfSymbolsMissing(symbols))
    onUpdate.foreach(_.throwIfSymbolsMissing(symbols))
  }

  def symbolTableDependencies =
    (expectations.flatMap(_.symbolTableDependencies)
      ++ onCreate.flatMap(_.symbolTableDependencies)
      ++ onUpdate.flatMap(_.symbolTableDependencies)).toSet - identifier
}