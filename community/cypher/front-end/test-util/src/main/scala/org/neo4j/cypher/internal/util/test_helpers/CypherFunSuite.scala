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
import org.scalatest.Canceled
import org.scalatest.Outcome
import org.scalatest.Status
import org.scalatest.Suite
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

abstract class CypherFunSuite
    extends Suite
    with Assertions
    with MockitoSugar
    with AnyFunSuiteLike
    with Matchers
    with BeforeAndAfterEach
    with CompareAsPrettyStrings {

  object Tags {

    // Exclusion is done natively in withFixture below to avoid JUnit Platform post-discovery
    // tag pruning on the ScalaTest tree (helmethair scalatest-junit-runner is not safe under it).

    /**
     * Use this tag to exclude tests from running with overridden default query language.
     * See the default-query-lang-cypher-25 maven profile.
     *
     * Note: must differ from the cypher25 profile's <excludedTestGroups> value to keep this tag out of the JUnit Platform
     */
    val NoQueryLangOverride: Tag = Tag("cypher.skip-on-query-language-override")

    /**
     * Use this tag to exclude tests from running with SPD.
     * See the test-spd maven profile.
     */
    val NoSpdOverride: Tag = Tag("exclude-spd-override")
  }

  override def withFixture(test: NoArgTest): Outcome = {
    val skipForSpd = test.tags.contains(Tags.NoSpdOverride.name) &&
      Option(System.getProperty("NEO4J_OVERRIDE_DBMS_TEST_FACTORY_SUPPLIER")).contains("spd")
    val skipForQueryLang = test.tags.contains(Tags.NoQueryLangOverride.name) &&
      Option(System.getProperty("NEO4J_OVERRIDE_QUERY_LANGUAGE")).contains("cypher_25")

    if (skipForSpd) {
      Canceled(s"Excluded under SPD test profile: ${test.name}")
    } else if (skipForQueryLang) {
      Canceled(s"Excluded under default-query-lang override profile: ${test.name}")
    } else super.withFixture(test)
  }

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

trait TestNameWithCaretPosition extends Suite {
  final def testName: String = caretPosition.cleanInput
  final def testPositions: Seq[InputPositionFromCaret] = caretPosition.positions

  private def caretPosition: CaretPosition = {
    if (__lastTestName == __testName) {
      __lastCaretPosition.get
    } else {
      __lastTestName = __testName
      __lastCaretPosition = Some(CaretPosition(__testName.get))
      __lastCaretPosition.get
    }
  }

  private var __lastCaretPosition: Option[CaretPosition] = None
  private var __lastTestName: Option[String] = None
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
