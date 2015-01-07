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
package org.neo4j.cypher.internal.commons

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.{JUnitSuiteLike, JUnitRunner}
import org.scalautils.LegacyTripleEquals
import org.junit.{After, Before}

// Shared between all TestSuite variants
abstract class CypherTestSuite extends Suite with Assertions with CypherTestSupport

abstract class CypherJUnitSuite extends CypherTestSuite with JUnitSuiteLike with LegacyTripleEquals {

  @Before
  final def beforeTest() {
    initTest()
  }

  @After
  final def afterTest() {
    stopTest()
  }
}

@RunWith(classOf[JUnitRunner])
abstract class CypherFunSuite extends CypherTestSuite with FunSuiteLike with Matchers with BeforeAndAfterEach {

  override protected def beforeEach() {
    initTest()
  }

  override protected def afterEach() {
    stopTest()
  }
}
