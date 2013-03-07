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

import org.neo4j.graphdb.{DynamicLabel, Relationship, Node}
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.pipes._
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.commands.RelationshipByIndex
import org.neo4j.cypher.internal.commands.IndexHint
import org.neo4j.cypher.internal.commands.NodeByIndex
import org.neo4j.cypher.internal.commands.NodeByIndexQuery
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.{InternalException, IndexHintException}

class EntityProducerFactory(planContext: PlanContext) {

  val nodeByIndex: PartialFunction[StartItem, EntityProducer[Node]] = {
    case NodeByIndex(varName, idxName, key, value) =>
      planContext.checkNodeIndex(idxName)
      (m: ExecutionContext, state: QueryState) => {
        val keyVal = key(m)(state).toString
        val valueVal = value(m)(state)
        state.query.nodeOps.indexGet(idxName, keyVal, valueVal)
      }
  }

  val nodeByIndexQuery: PartialFunction[StartItem, EntityProducer[Node]] = {
    case NodeByIndexQuery(varName, idxName, query) =>
      planContext.checkNodeIndex(idxName)
      (m: ExecutionContext, state: QueryState) => {
        val queryText = query(m)(state)
        state.query.nodeOps.indexQuery(idxName, queryText)
      }
  }

  val nodeById: PartialFunction[StartItem, EntityProducer[Node]] = {
    case NodeById(varName, ids) =>
      (m: ExecutionContext, state: QueryState) =>
        GetGraphElements.getElements[Node](ids(m)(state), varName, state.query.nodeOps.getById)
  }

  val nodeByIndexHint: PartialFunction[StartItem, EntityProducer[Node]] = {
    case IndexHint(identifier, labelName, propertyName, valueExp) =>
      val indexIdGetter = planContext.getIndexRuleId(labelName, propertyName)

      val indexId = indexIdGetter getOrElse
        (throw new IndexHintException(identifier, labelName, propertyName, "No such index found."))

      val expression = valueExp getOrElse
        (throw new InternalException("Something went wrong trying to build your query."))

      val label = DynamicLabel.label(labelName)

      (m: ExecutionContext, state: QueryState) => {
        val value = expression(m)(state)
        state.query.exactIndexSearch(indexId, value) // This is the real call. The next is not the call you are looking for
        state.query.nodeOps.all.filter {
          case node:Node => node.hasLabel(label) && node.getProperty(propertyName, null) == value
        }
      }
  }

  val relationshipByIndex: PartialFunction[StartItem, EntityProducer[Relationship]] = {
    case RelationshipByIndex(varName, idxName, key, value) =>
      planContext.checkRelIndex(idxName)
      (m: ExecutionContext, state: QueryState) => {
        val keyVal = key(m)(state).toString
        val valueVal = value(m)(state)
        state.query.relationshipOps.indexGet(idxName, keyVal, valueVal)
      }
  }

  val relationshipByIndexQuery: PartialFunction[StartItem, EntityProducer[Relationship]] = {
    case RelationshipByIndexQuery(varName, idxName, query) =>
      planContext.checkRelIndex(idxName)
      (m: ExecutionContext, state: QueryState) => {
        val queryText = query(m)(state)
        state.query.relationshipOps.indexQuery(idxName, queryText)
      }
  }
}