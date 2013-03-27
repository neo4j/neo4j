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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher.internal.commands.NodeByIndex
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.{IndexHintException, InternalException}
import org.neo4j.cypher.internal.data.SimpleVal._

class EntityProducerFactory {

  private def asProducer[T <: PropertyContainer](startItem: StartItem)
                                                (f: (ExecutionContext, QueryState) => Iterator[T]) =
    new EntityProducer[T] {
      def apply(m: ExecutionContext, q: QueryState) = f(m, q)

      def name = startItem.name

      def description = startItem.args.mapValues(fromStr).toSeq
    }

  def nodeStartItems = nodeById orElse
    nodeByIndex orElse
    nodeByIndexQuery orElse
    nodeByIndexHint orElse
    nodeByLabel

  val nodeByIndex: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ NodeByIndex(varName, idxName, key, value)) =>
      planContext.checkNodeIndex(idxName)

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
          val keyVal = key(m)(state).toString
          val valueVal = value(m)(state)
          state.query.nodeOps.indexGet(idxName, keyVal, valueVal)
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
        GetGraphElements.getElements[Node](ids(m)(state), varName, state.query.nodeOps.getById)
      }
  }

  val nodeByLabel: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ NodeByLabel(identifier, label)) if planContext.getLabelId(label).nonEmpty =>
      val labelId:Long = planContext.getLabelId(label).get

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) => state.query.getNodesByLabel(labelId) }
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


  val nodeByIndexHint: PartialFunction[(PlanContext, StartItem), EntityProducer[Node]] = {
    case (planContext, startItem @ SchemaIndex(identifier, labelName, propertyName, valueExp)) =>
      val indexIdGetter = planContext.getIndexRuleId(labelName, propertyName)

      val indexId = indexIdGetter getOrElse
        (throw new IndexHintException(identifier, labelName, propertyName, "No such index found."))

      val expression = valueExp getOrElse
        (throw new InternalException("Something went wrong trying to build your query."))

      asProducer[Node](startItem) { (m: ExecutionContext, state: QueryState) =>
        val value = expression(m)(state)
        state.query.exactIndexSearch(indexId, value)
      }
  }

  val relationshipByIndex: PartialFunction[(PlanContext, StartItem), EntityProducer[Relationship]] = {
    case (planContext, startItem @ RelationshipByIndex(varName, idxName, key, value)) =>
      planContext.checkRelIndex(idxName)
      asProducer[Relationship](startItem) { (m: ExecutionContext, state: QueryState) =>
        val keyVal = key(m)(state).toString
        val valueVal = value(m)(state)
        state.query.relationshipOps.indexGet(idxName, keyVal, valueVal)
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
        GetGraphElements.getElements[Relationship](ids(m)(state), varName, state.query.relationshipOps.getById)
      }
  }
}