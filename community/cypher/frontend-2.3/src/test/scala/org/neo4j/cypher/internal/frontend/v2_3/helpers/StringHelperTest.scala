/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.helpers

import org.apache.commons.lang3.SystemUtils
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class StringHelperTest extends CypherFunSuite {

  import StringHelper._

  test("should not fix position when the text contains no line break") {
    val text = "(line 1, column 8 (offset: 7))"

    text.fixPosition should equal("(line 1, column 8 (offset: 7))")
  }

  test("should fix positions on Windows after line breaks") {
    val text = "(line 3, column 8 (offset: 7))"

    if (SystemUtils.IS_OS_WINDOWS) {
      text.fixPosition should equal("(line 3, column 8 (offset: 9))")
    } else {
      text.fixPosition should equal("(line 3, column 8 (offset: 7))")
    }
  }
}
