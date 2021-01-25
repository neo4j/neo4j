/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.PropertyCursor
import org.neo4j.internal.kernel.api.RelationshipScanCursor
import org.neo4j.kernel.impl.query.QuerySubscriber

case class ProduceResultsPipe(source: Pipe, columns: Array[String])
                             (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {
  protected def internalCreateResults(input: ClosingIterator[CypherRow], state: QueryState): ClosingIterator[CypherRow] = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats
    val subscriber = state.subscriber
    if (state.prePopulateResults) {
      val nodeCursor = state.query.nodeCursor()
      val relCursor = state.query.relationshipScanCursor()
      val propertyCursor = state.query.propertyCursor()
      state.query.resources.trace(nodeCursor)
      state.query.resources.trace(relCursor)
      state.query.resources.trace(propertyCursor)
      input.map {
        original =>
          produceAndPopulate(original, subscriber, nodeCursor, relCursor, propertyCursor)
          original
      }
    } else
      input.map {
        original =>
          produce(original, subscriber)
          original
      }
  }

  private def produceAndPopulate(original: CypherRow, subscriber: QuerySubscriber, nodeCursor: NodeCursor, relCursor: RelationshipScanCursor, propertyCursor: PropertyCursor): Unit = {
    var i = 0
    subscriber.onRecord()
    while (i < columns.length) {
      val value = original.getByName(columns(i))
      ValuePopulation.populate(value, nodeCursor, relCursor, propertyCursor)
      subscriber.onField(i, value)
      i += 1
    }
    subscriber.onRecordCompleted()
  }

  private def produce(original: CypherRow, subscriber: QuerySubscriber): Unit = {
    var i = 0
    subscriber.onRecord()
    while (i < columns.length) {
      subscriber.onField(i, original.getByName(columns(i)))
      i += 1
    }
    subscriber.onRecordCompleted()
  }
}
