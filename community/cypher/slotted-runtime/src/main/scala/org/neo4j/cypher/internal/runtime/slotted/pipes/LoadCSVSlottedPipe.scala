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

import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.AbstractLoadCSVPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

case class LoadCSVSlottedPipe(
  source: Pipe,
  format: CSVFormat,
  urlExpression: Expression,
  refSlotOffset: Int,
  metaDataSlotOffset: Int,
  fieldTerminator: Option[String],
  legacyCsvQuoteEscaping: Boolean,
  bufferSize: Int
)(val id: Id = Id.INVALID_ID)
    extends AbstractLoadCSVPipe(source, format, urlExpression, fieldTerminator, legacyCsvQuoteEscaping, bufferSize) {

  final override def writeRow(
    filename: String,
    linenumber: Long,
    last: Boolean,
    argumentRow: CypherRow,
    value: AnyValue
  ): CypherRow = {
    val newRow = rowFactory.copyWith(argumentRow)
    newRow.setRefAt(refSlotOffset, value)
    newRow.setRefAt(
      metaDataSlotOffset,
      ResourceLinenumber(filename, linenumber, last)
    ) // Always overwrite linenumber if we have nested LoadCsvs
    newRow
  }
}
