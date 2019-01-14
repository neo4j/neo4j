/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen

/**
  * Operator which loads a CSV from some URL. For every source row, the CSV is loaded. Each CSV line is produced as a
  * row consisting of the current source row + one value holding the CSV line data.
  *
  * If the CSV file has headers, each line will represented in Cypher as a MapValue, if the file has no header, each
  * line will be a ListValue.
  */
case class LoadCSV(source: LogicalPlan,
                   url: Expression,
                   variableName: String,
                   format: CSVFormat,
                   fieldTerminator: Option[String],
                   legacyCsvQuoteEscaping: Boolean,
                   csvBufferSize: Int)
                  (implicit idGen: IdGen) extends LogicalPlan(idGen) {

  override val availableSymbols: Set[String] = source.availableSymbols + variableName

  override def lhs = Some(source)

  override def rhs = None

  override def strictness: StrictnessMode = source.strictness
}
