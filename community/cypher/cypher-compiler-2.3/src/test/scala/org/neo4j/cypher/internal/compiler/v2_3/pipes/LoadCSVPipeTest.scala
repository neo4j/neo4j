/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import java.net.{URI, URL}
import java.nio.file.Paths

import org.apache.commons.lang3.SystemUtils
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols.CTNumber
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LoadCSVPipeTest extends CypherFunSuite with PipeTestSupport {

  test("should map unix path") {
    val url = if (SystemUtils.IS_OS_WINDOWS)
      new URL("file:///C:/tmp/data/import.csv")
    else
      new URL("file:///tmp/data/import.csv")

    val path = Paths.get(url.toURI)

    val prefix = if (SystemUtils.IS_OS_WINDOWS)
      "C:\\tmp"
    else
      "/tmp"
    path.toAbsolutePath should equal(Paths.get(prefix, "data", "import.csv"))
  }
}
