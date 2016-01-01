/**
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.helpers.TxCounts
import java.io.PrintWriter
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.StringHelper.RichString

class PeriodicCommitLoadCsvAcceptanceTest
  extends ExecutionEngineFunSuite with CreateTempFileTestSupport with TxCountsTrackingTestSupport {

  test("should tell line number information when failing using periodic commit and load csv") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
      writer.println("0")
      writer.println("3")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '${url}' AS line " +
      s"CREATE ({name: 1/toInt(line[0])})"

    // when executing 5 updates
    val e = intercept[CypherException](execute(queryText))

    // then
    e.getMessage should include("on line 3. Possibly the last row committed during import is line 2. Note that this information might not be accurate.")
  }

  private def createFile(f: PrintWriter => Unit) = createTempFileURL("cypher", ".csv")(f).cypherEscape
}
