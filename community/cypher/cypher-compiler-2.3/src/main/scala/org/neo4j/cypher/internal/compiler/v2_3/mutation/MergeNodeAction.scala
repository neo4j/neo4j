/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.mutation

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{ReadsAllNodes, WritesAnyNode, WritesNodesWithLabels, ReadsNodesWithLabels, Effects}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.PropertySupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{EntityProducer, QueryState}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, InvalidSemanticsException, MergeConstraintConflictException}
import org.neo4j.graphdb.Node

final case class IndexNodeProducer(label: KeyToken, propertyKey: KeyToken, producer: EntityProducer[Node]) extends EntityProducer[Node] {
  def producerType: String = s"IndexNodProducer(${producer.producerType})"
  def arguments = producer.arguments
  def apply(ctx: ExecutionContext, state: QueryState) : Iterator[Node] = producer(ctx, state)
  override def toString() = s":${label.name}.${propertyKey.name}" //":Person.name"
}

sealed abstract class MergeNodeProducer {
  def arguments: Seq[Argument]
}

final case class PlainMergeNodeProducer(nodeProducer: EntityProducer[Node]) extends MergeNodeProducer {
  def arguments = nodeProducer.arguments
}

final case class UniqueMergeNodeProducers(nodeProducers: Seq[IndexNodeProducer]) extends MergeNodeProducer {
  def arguments: Seq[Argument] = nodeProducers.flatMap(_.arguments)
}

case class MergeNodeAction(identifier: String,
                           props: Map[KeyToken, Expression],
                           labels: Seq[KeyToken],
                           expectations: Seq[Predicate],
                           onCreate: Seq[UpdateAction],
                           onMatch: Seq[UpdateAction],
                           maybeNodeProducer: Option[MergeNodeProducer]) extends UpdateAction {

  def children = expectations ++ onCreate ++ onMatch

  lazy val definedProducer: MergeNodeProducer = maybeNodeProducer.getOrElse(
    throw new InternalException("Tried to run merge action without finding node producer. This should never happen. " +
      "It seems the execution plan builder failed. ") )

  def exec(context: ExecutionContext, state: QueryState): Iterator[ExecutionContext] = {

    val foundNodes: Iterator[ExecutionContext] = findNodes(context)(state)

    // TODO get rid of double evaluation of property expressions
    ensureNoNullNodeProperties(context, state)

    if (foundNodes.isEmpty) {
      val query: QueryContext = state.query
      val createdNode: Node = query.createNode()
      val newContext = context += (identifier -> createdNode)

      onCreate.foreach {
        action => action.exec(newContext, state)
      }

      Iterator(newContext)
    } else {
      foundNodes.map {
        nextContext =>
          onMatch.foreach(_.exec(nextContext, state))
          nextContext
      }
    }
  }

  def ensureNoNullNodeProperties(context: ExecutionContext, state: QueryState) {
    PropertySupport.firstNullPropertyIfAny(props, context, state) match {
      case Some(key) =>
        throw new InvalidSemanticsException(s"Cannot merge node using null property value for ${key.name}")
      case None =>
      // awesome
    }
  }

  override def arguments: Seq[Argument] = {
    val producers: Seq[Argument] = maybeNodeProducer.map(_.arguments).toSeq.flatten
    super.arguments ++ producers
  }

  def findNodes(context: ExecutionContext)(implicit state: QueryState): Iterator[ExecutionContext] = definedProducer match {
    // fetch nodes from source
    case PlainMergeNodeProducer(nodeProducer) =>
      nodeProducer(context, state).
        map(n => context.newWith(identifier -> n)).
        filter(ctx => expectations.forall(_.isTrue(ctx)(state)))

    // unique index lookup
    case UniqueMergeNodeProducers(indexNodeProducers) =>
      val firstProducer = indexNodeProducers.head
      val checkedOptNode: Option[Node] = optNode(firstProducer(context, state))

      indexNodeProducers.tail.foreach { (producer: EntityProducer[Node]) =>
        optNode(producer(context, state)) match {
          case Some(node) if checkedOptNode.isDefined =>
            val firstId = checkedOptNode.get.getId
            val foundId = node.getId
            if (firstId != foundId) {
              throw new MergeConstraintConflictException(s"Merge did not find a matching node and can not create a new node due to conflicts with existing unique nodes. The conflicting constraints are on: $firstProducer and $producer")
            }
          case None =>
            if (checkedOptNode.isDefined) {
              throw new MergeConstraintConflictException(s"Merge did not find a matching node and can not create a new node due to conflicts with both existing and missing unique nodes. The conflicting constraints are on: $firstProducer and $producer")
            }
          case _ => false
        }
      }

      checkedOptNode match {
        case Some(node) =>
          val resultContext = context.newWith(identifier -> node)
          if (expectations.forall(_.isTrue(resultContext))) Iterator(resultContext) else Iterator.empty
        case None =>
          Iterator.empty
      }
  }

  private def optNode(iterator: Iterator[Node]): Option[Node] =
    if (iterator.hasNext) {
      val node = iterator.next()
      if (iterator.hasNext) {
        throw new InternalException("We got more than one node back from a unique index lookup")
      }
      Some(node)
    } else {
      None
    }

  def identifiers: Seq[(String, CypherType)] = Seq(identifier -> CTNode)

  def rewrite(f: (Expression) => Expression) =
    MergeNodeAction(identifier = identifier,
      props = props.map { case (k, v) => k.rewrite(f) -> v.rewrite(f) },
      labels = labels.map(_.rewrite(f)),
      expectations = expectations.map(_.rewriteAsPredicate(f)),
      onCreate = onCreate.map(_.rewrite(f)),
      onMatch = onMatch.map(_.rewrite(f)),
      maybeNodeProducer = maybeNodeProducer)

  def symbolTableDependencies =
    (expectations.flatMap(_.symbolTableDependencies)
      ++ onCreate.flatMap(_.symbolTableDependencies)
      ++ onMatch.flatMap(_.symbolTableDependencies)).toSet - identifier

  def localEffects(symbols: SymbolTable) =
    if (labels.isEmpty) Effects(WritesAnyNode, ReadsAllNodes)
    else Effects(ReadsNodesWithLabels(labels.map(_.name).toSet), WritesNodesWithLabels(labels.map(_.name).toSet))

  override def updateSymbols(symbol: SymbolTable): SymbolTable = symbol.add(identifiers.toMap)
}
