/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue

import scala.jdk.CollectionConverters.CollectionHasAsScala

case class RemoveLabelsPipe(src: Pipe, variable: String, labels: Seq[LazyLabel], dynamicLabels: Seq[Expression])(
  val id: Id = Id.INVALID_ID
) extends PipeWithSource(src) with GraphElementPropertyFunctions {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.map { row =>
      val item = row.getByName(variable)
      if (!(item eq Values.NO_VALUE)) removeLabels(row, state, CastSupport.castOrFail[VirtualNodeValue](item).id)
      row
    }
  }

  private def removeLabels(row: CypherRow, state: QueryState, nodeId: Long): Int = {
    val labelIds = labels.map(_.getId(state.query)) ++ dynamicLabels.flatMap(l =>
      CypherFunctions.asStringList(l(row, state)).asScala.map(l =>
        state.query.getOrCreateLabelId(l)
      )
    ).filter(_ != LazyLabel.UNKNOWN)
    state.query.removeLabelsFromNode(nodeId, labelIds.iterator)
  }
}
