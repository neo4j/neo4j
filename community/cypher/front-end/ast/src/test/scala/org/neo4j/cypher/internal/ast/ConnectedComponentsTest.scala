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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.connectedComponents.ComponentPart
import org.neo4j.cypher.internal.ast.connectedComponents.ConnectedComponent
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ConnectedComponentsTest extends CypherFunSuite {

  test("(a)->(b), (c)->(d) has two connected components") {
    val disconnected = connectedComponents(Vector(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("c"), varFor("d"))
    ))

    disconnected should equal(Vector(
      ConnectedComponent(ComponentPart(varFor("a"), varFor("b"))),
      ConnectedComponent(ComponentPart(varFor("c"), varFor("d")))
    ))
  }

  test("(a)->(b)->(c) does contain one connected component") {
    val connected = connectedComponents(Vector(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("b"), varFor("c"))
    ))

    connected should equal(Vector(
      ConnectedComponent(ComponentPart(varFor("a"), varFor("b")), ComponentPart(varFor("b"), varFor("c")))
    ))
  }

  test("(a)->(b)->(c)->(d) does only contain one component") {
    val connected = connectedComponents(Vector(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("b"), varFor("c")),
      ComponentPart(varFor("c"), varFor("d"))
    ))

    connected shouldBe Vector(ConnectedComponent(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("b"), varFor("c")),
      ComponentPart(varFor("c"), varFor("d"))
    ))
  }

  test("(a)->(b)->(c)-(a) contains one component ") {
    val connected = connectedComponents(Vector(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("b"), varFor("c")),
      ComponentPart(varFor("c"), varFor("a"))
    ))

    connected shouldBe Vector(ConnectedComponent(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("b"), varFor("c")),
      ComponentPart(varFor("c"), varFor("a"))
    ))
  }

  test("(a)->(b)->(c), (b)->(d) contains one component ") {
    val connected = connectedComponents(Vector(
      ComponentPart(varFor("a"), varFor("b"), varFor("c")),
      ComponentPart(varFor("b"), varFor("d"))
    ))

    connected shouldBe Vector(ConnectedComponent(
      ComponentPart(varFor("a"), varFor("b"), varFor("c")),
      ComponentPart(varFor("b"), varFor("d"))
    ))
  }

  test("(a)->(b), (c)->(d), (d)->(a) contains one component ") {
    val connected = connectedComponents(Vector(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("c"), varFor("d")),
      ComponentPart(varFor("d"), varFor("a"))
    ))

    connected shouldBe Vector(ConnectedComponent(
      ComponentPart(varFor("a"), varFor("b")),
      ComponentPart(varFor("c"), varFor("d")),
      ComponentPart(varFor("d"), varFor("a"))
    ))
  }

  private def varFor(name: String): LogicalVariable = Variable(name)(null)
}
