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
package org.neo4j.cypher.internal.frontend.v2_3.test_helpers

import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.scalatest._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

import scala.reflect.Manifest

@RunWith(classOf[JUnitRunner])
abstract class CypherFunSuite
  extends Suite
  with Assertions
  with CypherTestSupport
  with MockitoSugar
  with FunSuiteLike
  with Matchers
  with BeforeAndAfterEach {

  override protected def beforeEach() {
    initTest()
  }

  override protected def afterEach() {
    stopTest()
  }

  def argCaptor[T <: AnyRef](implicit manifest: Manifest[T]): ArgumentCaptor[T] = {
    ArgumentCaptor.forClass(manifest.runtimeClass.asInstanceOf[Class[T]])
  }
}

trait TestName extends Suite {
  final def testName = __testName.get

  private var __testName: Option[String] = None

  override protected def runTest(testName: String, args: Args): Status = {
    __testName = Some(testName)
    try {
      super.runTest(testName, args)
    } finally {
      __testName = None
    }
  }
}
