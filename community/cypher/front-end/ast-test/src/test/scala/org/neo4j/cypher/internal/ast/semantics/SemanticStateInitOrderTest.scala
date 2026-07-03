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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

class SemanticStateInitOrderTest extends CypherFunSuite {

  test("SemanticState initializes when ScopeZipper loads first") {
    val javaBin = ProcessHandle.current.info.command.orElseThrow()
    val classpath = System.getProperty("java.class.path")
    val mainClass = classOf[SemanticStateInitOrderProbe.type].getName.stripSuffix("$")
    val process = new ProcessBuilder(javaBin, "-cp", classpath, mainClass)
      .redirectErrorStream(true)
      .start()

    val exited = process.waitFor(30, TimeUnit.SECONDS)
    if (!exited) {
      process.destroyForcibly()
      fail("SemanticStateInitOrderProbe did not exit within timeout")
    }

    val output = new String(process.getInputStream.readAllBytes(), UTF_8)
    withClue(output) {
      process.exitValue() shouldBe 0
    }
    output should include("OK")
  }
}
