/**
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
package org.neo4j.cypher.internal.compiler.v2_0

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.scalatest.BeforeAndAfter

class CleanUpperTest extends CypherFunSuite with BeforeAndAfter {
  var cleanUpper: CleanUpper = _
  var ran = false

  before {
    cleanUpper = new CleanUpper
    ran = false
  }

  test("cleanUp call methods") {

    cleanUpper.addCleanupTask(() => ran = true)
    cleanUpper.cleanUp()

    ran should equal(true)
  }

  test("cleanUp calls all cleanUp methods even if some fail") {

    cleanUpper.addCleanupTask(() => throw new Exception("oh noes"))
    cleanUpper.addCleanupTask(() => ran = true)

    intercept[Exception](cleanUpper.cleanUp())

    ran should equal(true)
  }

  test("cleanUp calls all cleanUp and if there are failures the first exception is thrown") {

    val expected = new Exception("oh noes")
    cleanUpper.addCleanupTask(() => throw expected)
    cleanUpper.addCleanupTask(() => throw new Exception)

    val ex = intercept[Exception](cleanUpper.cleanUp())

    ex should equal(expected)
  }

  test("cleanup without any cleanups does not fail") {
    cleanUpper.cleanUp()

    ran should equal(false)
  }

}
