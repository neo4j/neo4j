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
package org.neo4j.cypher.internal.commons

import org.scalatest._
import org.junit.runner.{Runner, RunWith}
import org.scalatest.junit.{JUnitSuiteLike, JUnitRunner}
import org.scalautils.LegacyTripleEquals
import org.junit.{BeforeClass, After, Before}

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
abstract class CypherFunSuite extends CypherTestSuite
  with FunSuiteLike with Matchers with BeforeAndAfterEach {

  // calling ensureReady in an initializer block makes it work when run
  // from either the JUnit or the scala test plugin in IDEA
  ensureReady()

  override protected def beforeEach() {
    initTest()
  }

  override protected def afterEach() {
    stopTest()
  }

  protected def ensureReady() = {
    val runnerClass = runWith.getOrElse( throw new IllegalStateException("Scala test not annotated with @RunWith") )
    if (classOf[JUnitRunner].isAssignableFrom(runnerClass.value())) {
      throw new IllegalStateException("Scala test not annotated to be executed by JUnit")
    }
  }

  protected def runWith: Option[RunWith] = {
    val annotation = getClass.getAnnotation(classOf[RunWith])
    if (annotation == null) Some(annotation) else None
  }
}
