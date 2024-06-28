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
package org.neo4j.gqlstatus

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GqlStatusInfoScalaTest extends AnyFunSuite with Matchers {

  test("String is formatted correctly") {
    val m = "Hello my name is `$name`"
    val result = GqlStatusInfoCodes.STATUS_00000.toJavaFormattable(m)
    result shouldBe "Hello my name is `%s`"
  }
}
