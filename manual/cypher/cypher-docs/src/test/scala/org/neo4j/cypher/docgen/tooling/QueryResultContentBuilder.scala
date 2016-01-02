/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.docgen.tooling

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v3_0.Rewritable._
import org.neo4j.cypher.internal.helpers.GraphIcing

/**
 * This class is responsible for replacing the Content tags asking for query results
 * with the actual results from running the queries, formatted according to the normal
 * textual output of ExecutionResultDumper
 */
class QueryResultContentBuilder(valueFormatter: Any => String)
  extends (InternalExecutionResult => Content) with GraphIcing {

  override def apply(result: InternalExecutionResult): Content = {

    val columns = result.columns
    var rowCount = 0

    /* Need to do .toList here, to see the results. The iterator has been emptied,
     but it is a RewindableExecutionResult we have here that can still provide
     the backing List. Yeah, it's a hack, but it allows us to both assert on the
     results and produce text output */
    val rows = result.toList.map { resultRow =>
      rowCount += 1
      val values = columns.map { key =>
        val value = resultRow(key)
        valueFormatter(value)
      }.toSeq
      ResultRow(values)
    }

    val footerRows = if (rowCount == 1) "1 row" else s"$rowCount rows"
    val footer = if (result.queryStatistics().containsUpdates)
      footerRows + "\n" + result.queryStatistics().toString
    else
      footerRows

    QueryResultTable(result.columns, rows, footer)
  }
}
