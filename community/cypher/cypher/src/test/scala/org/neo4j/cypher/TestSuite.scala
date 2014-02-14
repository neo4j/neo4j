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
package org.neo4j.cypher

import org.scalatest._
import org.scalatest.junit.{JUnitSuiteLike, JUnitSuite}
import org.scalautils.LegacyTripleEquals
import org.junit.{Test, After, Before}

// Inherited by test mixin classes that need to manage resources
trait TestSupport  {
  protected def initTest() {}
  protected def stopTest() {}
}

// Shared between all TestSuite variants
trait TestSuite extends Suite with Assertions with TestSupport

trait JUnitTestSuite extends TestSuite with JUnitSuiteLike with LegacyTripleEquals {

  @Before
  final def beforeTest() {
    initTest()
  }

  @After
  final def afterTest() {
    stopTest()
  }
}

trait FunTestSuite extends TestSuite with FunSuiteLike with Matchers with BeforeAndAfterEach {

  override protected def beforeEach() {
    initTest()
  }

  override protected def afterEach() {
    stopTest()
  }
}

trait GraphDatabaseJUnitSuite
  extends JUnitTestSuite with GraphDatabaseTestSupport

trait ExecutionEngineJUnitSuite
  extends JUnitTestSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport

trait ExecutionEngineFunSuite
  extends FunTestSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport
