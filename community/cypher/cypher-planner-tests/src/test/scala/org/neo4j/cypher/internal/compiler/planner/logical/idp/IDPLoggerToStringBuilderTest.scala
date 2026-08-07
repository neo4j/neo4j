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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.scalatest.OptionValues

class IDPLoggerToStringBuilderTest extends CypherPlannerTestSuite with OptionValues {

  test("should log messages") {
    val l = new IDPLoggerToStringBuilder()
    l.log("no scope")
    l.markScope("hello") {
      l.log("inside hello")
      l.markScope("world") {
        l.log("inside world")
        l.log("multiple\nlines\ninside")
      }
      l.log("back to hello")
    }
    l.log("no scope again")

    l.result().value.linesIterator.toVector shouldEqual
      """no scope
        |> BEGIN hello
        |⁞ inside hello
        |⁞ > BEGIN world
        |⁞ ⁞ inside world
        |⁞ ⁞ multiple
        |⁞ ⁞ lines
        |⁞ ⁞ inside
        |⁞ < END world
        |⁞ back to hello
        |< END hello
        |no scope again""".stripMargin.linesIterator.toVector
  }
}
