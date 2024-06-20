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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.reflections.Reflections

import scala.jdk.CollectionConverters.SetHasAsScala

class CypherScopeTest extends CypherFunSuite {

  test("convert to kernel cypher scope") {
    CypherScope.All.foreach { scope =>
      CypherScope.toKernelScope(scope) should not be null
    }
  }

  test("one to one mapping to kernel scopes") {
    CypherScope.All.map(CypherScope.toKernelScope) shouldBe org.neo4j.kernel.api.CypherScope.ALL_SCOPES.asScala
    CypherScope.All.map(CypherScope.toKernelScope) shouldBe org.neo4j.kernel.api.CypherScope.values().toSet
  }

  test("CypherScope.All should include everything") {
    CypherScope.All.map(_.getClass) shouldBe new Reflections("org.neo4j.cypher.internal.frontend.phases")
      .getSubTypesOf[CypherScope](classOf[CypherScope])
      .asScala
  }
}
