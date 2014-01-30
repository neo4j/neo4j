/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0.symbols.{CollectionType, AnyType, MapType, SymbolTable}
import org.neo4j.cypher.internal.compiler.v2_0.{CleanupTask, ExecutionContext, PlanDescription}
import java.io.{InputStreamReader, BufferedReader, FileReader}
import au.com.bytecode.opencsv.CSVReader
import java.net.URL

class LoadCSVPipe(source: Pipe, withHeaders: Boolean, urlString: String, identifier: String) extends PipeWithSource(source) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap(context => {
      val url = new URL(urlString)
      val reader = new BufferedReader(new InputStreamReader(url.openStream()))
      val csvReader = new CSVReader(reader)
      state.addCleanupTask(new CleanupTask { def close() { reader.close() } })

      new Iterator[ExecutionContext] {
        private var nextRow: Array[String] = csvReader.readNext()

        def hasNext: Boolean =
          nextRow != null


        def next(): ExecutionContext = {
          if (nextRow == null) Iterator.empty.next()
          val newContext = context.newWith(identifier -> nextRow.toSeq)
          nextRow = csvReader.readNext()
          newContext
        }
      }
    })
  }

  def executionPlanDescription: PlanDescription = {
    source.executionPlanDescription.andThen(this, "LoadCSV")
  }

  def symbols: SymbolTable = {
    val typ = if (withHeaders) {
      MapType.instance
    } else {
      CollectionType(AnyType.instance)
    }
    source.symbols.add(identifier, typ)
  }
}
