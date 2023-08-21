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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AbstractTransactionForeachPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionForeachPipe.toStatusMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionStatus
import org.neo4j.cypher.internal.util.attribution.Id

case class TransactionForeachSlottedPipe(
  source: Pipe,
  inner: Pipe,
  batchSize: Expression,
  onErrorBehaviour: InTransactionsOnErrorBehaviour,
  statusSlot: Option[Slot]
)(val id: Id = Id.INVALID_ID) extends AbstractTransactionForeachPipe(source, inner, batchSize, onErrorBehaviour) {
  private[this] val statusOffsetOpt = statusSlot.map(_.offset)

  override protected def withStatus(
    output: ClosingIterator[CypherRow],
    status: TransactionStatus
  ): ClosingIterator[CypherRow] = {
    statusOffsetOpt match {
      case Some(statusOffset) => output.withVariable(statusOffset, toStatusMap(status))
      case _                  => output
    }
  }
}
