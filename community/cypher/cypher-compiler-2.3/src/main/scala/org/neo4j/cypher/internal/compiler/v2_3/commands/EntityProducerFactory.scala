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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.GetGraphElements
import org.neo4j.cypher.internal.compiler.v2_3.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{NodeByLabelEntityProducer, EntityProducer, IndexSeekModeFactory, QueryState}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Argument
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.{EntityNotFoundException, IndexHintException, InternalException}
import org.neo4j.graphdb.{Node, PropertyContainer, Relationship}

class EntityProducerFactory extends GraphElementPropertyFunctions {

  private def asProducer[T <: PropertyContainer](startItem: StartItem)
                                                (f: (ExecutionContext, QueryState) => Iterator[T]) =
    new EntityProducer[T] {
      def apply(m: ExecutionContext, q: QueryState) = f(m, q)

      def producerType = startItem.producerType

      def arguments: Seq[Argument] = startItem.arguments
    }

  def readNodeStartItems: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] =
    nodeById orElse
      nodeByIndex orElse
      nodeByIndexQuery orElse
      nodeByIndexHint(readOnly = true) orElse
      nodeByLabel orElse
      nodesAll

  def updateNodeStartItems: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] =
    nodeById orElse
      nodeByIndex orElse
      nodeByIndexQuery orElse
      nodeByIndexHint(readOnly = false) orElse
      nodeByLabel orElse
      nodesAll

  val nodeByIndex: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ NodeByIndex(varName, idxName, key, value)) =>
      planContext.checkNodeIndex(idxName)

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        val keyVal = key(m)(state).toString
        val valueVal = value(m)(state)
        val neoValue = makeValueNeoSafe(valueVal)
        state.query.nodeOps.indexGet(idxName, keyVal, neoValue)
      }
  }

  val nodeByIndexQuery: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ NodeByIndexQuery(varName, idxName, query)) =>
      planContext.checkNodeIndex(idxName)
      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        val queryText = query(m)(state)
        state.query.nodeOps.indexQuery(idxName, queryText)
      }
  }

  val nodeById: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ NodeById(varName, ids)) =>
      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        GetGraphElements.getElements[Node](ids(m)(state), varName, (id) =>
          state.query.nodeOps.getById(id))
      }
    case (planContext, startItem@NodeByIdOrEmpty(varName, ids)) =>
      asProducer[Node](startItem) {
        (m: ExecutionContext, state: QueryState) =>
          val idsVal: Any = ids(m)(state)
          GetGraphElements.getOptionalElements[Node](idsVal, varName, (id) =>
            try {
              Some(state.query.nodeOps.getById(id))
            } catch {
              case _: EntityNotFoundException => None
            })
      }
  }

  val nodeByLabel: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    // The label exists at compile time - no need to look up the label id for every run
    case (planContext, startItem@NodeByLabel(identifier, label)) if planContext.getOptLabelId(label).nonEmpty =>
      val labelId: Int = planContext.getOptLabelId(label).get

      NodeByLabelEntityProducer(startItem, labelId)

    // The label is missing at compile time - we look it up every time this plan is run
    case (planContext, startItem@NodeByLabel(identifier, label)) => asProducer(startItem) {
      (m: ExecutionContext, state: QueryState) =>
        state.query.getOptLabelId(label) match {
          case Some(labelId) => state.query.getNodesByLabel(labelId)
          case None          => Iterator.empty
        }
    }
  }

  val nodesAll: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ AllNodes(identifier)) =>
      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) => state.query.nodeOps.all }
  }

  val relationshipsAll: PartialFunction[(PlanContext, StartItem), EntityProducer[Relationship]] = {
    case (planContext, startItem @ AllRelationships(identifier)) =>
      asProducer[Relationship](startItem) { (m: ExecutionContext, state: QueryState) =>
        state.query.relationshipOps.all }
  }

  def nodeByIndexHint(readOnly: Boolean): PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ SchemaIndex(identifier, labelName, propertyName, AnyIndex, Some(ScanQueryExpression(_)))) =>

      val indexGetter = planContext.getIndexRule(labelName, propertyName)

      val index = indexGetter getOrElse
        (throw new IndexHintException(identifier, labelName, propertyName, "No such index found."))

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        val resultNodes: Iterator[Node] = state.query.indexScan(index)
        resultNodes
      }

    case (planContext, startItem @ SchemaIndex(identifier, labelName, propertyName, AnyIndex, valueExp)) =>

      val indexGetter = planContext.getIndexRule(labelName, propertyName)

      val index = indexGetter getOrElse
        (throw new IndexHintException(identifier, labelName, propertyName, "No such index found."))

      val expression = valueExp getOrElse
        (throw new InternalException("Something went wrong trying to build your query."))
      val indexFactory = IndexSeekModeFactory(unique = false, readOnly = readOnly).
        fromQueryExpression(expression).
        indexFactory(index)

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        indexQuery(expression, m, state, indexFactory(state), labelName, propertyName)
      }

    case (planContext, startItem @ SchemaIndex(identifier, labelName, propertyName, UniqueIndex, valueExp)) =>

      val indexGetter = planContext.getUniqueIndexRule(labelName, propertyName)

      val index = indexGetter getOrElse
        (throw new IndexHintException(identifier, labelName, propertyName, "No such index found."))

      val expression = valueExp getOrElse
        (throw new InternalException("Something went wrong trying to build your query."))
      val indexFactory = IndexSeekModeFactory(unique = true, readOnly = readOnly).
        fromQueryExpression(expression).
        indexFactory(index)

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        indexQuery(expression, m, state, indexFactory(state), labelName, propertyName)
      }
  }

  val relationshipByIndex: PartialFunction[(PlanContext, StartItem), EntityProducer[Relationship]] = {
    case (planContext, startItem @ RelationshipByIndex(varName, idxName, key, value)) =>
      planContext.checkRelIndex(idxName)
      asProducer[Relationship](startItem) { (m: ExecutionContext, state: QueryState) =>
        val keyVal = key(m)(state).toString
        val valueVal = value(m)(state)
        val neoValue = makeValueNeoSafe(valueVal)
        state.query.relationshipOps.indexGet(idxName, keyVal, neoValue)
      }
  }

  val relationshipByIndexQuery: PartialFunction[(PlanContext, StartItem), EntityProducer[Relationship]] = {
    case (planContext, startItem @ RelationshipByIndexQuery(varName, idxName, query)) =>
      planContext.checkRelIndex(idxName)
      asProducer[Relationship](startItem) { (m: ExecutionContext, state: QueryState) =>
        val queryText = query(m)(state)
        state.query.relationshipOps.indexQuery(idxName, queryText)
      }
  }

  val relationshipById: PartialFunction[(PlanContext, StartItem), EntityProducer[Relationship]] = {
    case (planContext, startItem @ RelationshipById(varName, ids)) =>
      asProducer[Relationship](startItem) { (m: ExecutionContext, state: QueryState) =>
        GetGraphElements.getElements[Relationship](ids(m)(state), varName, (id) =>
          state.query.relationshipOps.getById(id))
      }
  }

  object NO_NODES extends EntityProducer[Node] {
    def producerType: String = "NoNodes"

    def apply(v1: ExecutionContext, v2: QueryState) = Iterator.empty

    def arguments: Seq[Argument] = Seq.empty
  }
}