/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.test_helpers

import org.mockito.ArgumentCaptor
import org.scalatest.Args
import org.scalatest.Assertions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Status
import org.scalatest.Suite
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

abstract class CypherFunSuite
    extends Suite
    with Assertions
    with MockitoSugar
    with AnyFunSuiteLike
    with Matchers
    with BeforeAndAfterEach {

  def argCaptor[T <: AnyRef](implicit manifest: Manifest[T]): ArgumentCaptor[T] = {
    ArgumentCaptor.forClass(manifest.runtimeClass.asInstanceOf[Class[T]])
  }

  protected def normalizeNewLines(string: String) = {
    string.replace("\r\n", "\n")
  }
}

trait TestName extends Suite {
  final def testName: String = __testName.get

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
