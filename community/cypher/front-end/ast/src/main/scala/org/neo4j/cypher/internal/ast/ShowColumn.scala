/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType

case class ShowColumn(variable: LogicalVariable, cypherType: CypherType, name: String)

object ShowColumn {

  def apply(name: String, cypherType: CypherType = CTString)(position: InputPosition): ShowColumn =
    ShowColumn(Variable(name)(position), cypherType, name)
}

case class DefaultOrAllShowColumns(useAllColumns: Boolean, columns: List[ShowColumn])

object DefaultOrAllShowColumns {

  type ShowByDefault = Boolean

  def apply(columns: List[(ShowColumn, ShowByDefault)], yieldOrWhere: YieldOrWhere): DefaultOrAllShowColumns = {

    val briefShowColumns = columns.filter(_._2).map(_._1)
    val allShowColumns = columns.map(_._1)

    val allColumns = yieldOrWhere match {
      case Some(Left(_)) => true
      case _             => false
    }

    apply(allColumns, briefShowColumns, allShowColumns)
  }

  def apply(useAllColumns: Boolean, brief: List[ShowColumn], all: List[ShowColumn]): DefaultOrAllShowColumns = {
    if (useAllColumns) DefaultOrAllShowColumns(useAllColumns, all) else DefaultOrAllShowColumns(useAllColumns, brief)
  }

}
