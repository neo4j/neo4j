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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Random

object CypherVersionHelpers {

  @deprecated("Please do not use this, find other ways to handle multi language testing", "now")
  def randomVersion(): CypherVersion = {
    val values = CypherVersion.values()
    values(Random.nextInt(values.length))
  }

  @deprecated("Please do not use this, find other ways to handle multi language testing", "now")
  def arbitrarySemanticContext(): SemanticCheckContext =
    SemanticCheckContext(randomVersion(), NotImplementedErrorMessageProvider)

  def versionedSemanticContext(language: CypherVersion): SemanticCheckContext =
    SemanticCheckContext(language, NotImplementedErrorMessageProvider)

  def equalInVersions[T](versions: CypherVersion*)(f: CypherVersion => T): T = {
    val baselineVersion = versions(Random.nextInt(versions.size))
    versions.foldLeft(f(baselineVersion)) {
      case (baseline, `baselineVersion`) => baseline
      case (baseline, version) =>
        val result = f(version)
        val clue =
          s"""Expected the same value but got:
             |CYPHER $baselineVersion: $baseline
             |CYPHER $version: $result
             |""".stripMargin
        if (!(result == baseline)) throw new AssertionError(clue)
        baseline
    }
  }

  def equalInAllVersions[T](f: CypherVersion => T): T = equalInVersions(CypherVersion.values(): _*)(f)
}

trait CypherVersionTestSupport {
  self: CypherFunSuite =>

  def testVersions(testName: String)(f: CypherVersion => Any): Unit =
    test(testName) {
      CypherVersion.values().foreach(v => withClue(s"CYPHER $v\n")(f(v)))
    }

  def testVersionsExcept5(testName: String)(f: CypherVersion => Any): Unit =
    test(testName) {
      versionsExcept5Iterable.foreach(v =>
        withClue(s"CYPHER $v\n")(f(v))
      )
    }

  def versionsExcept5(f: CypherVersion => Any): Unit =
    versionsExcept5Iterable.foreach(v => withClue(s"CYPHER $v\n")(f(v)))

  def versionsExcept5Iterable: Iterable[CypherVersion] = {
    CypherVersion.values().filter(version => version != CypherVersion.Cypher5)
  }
}
