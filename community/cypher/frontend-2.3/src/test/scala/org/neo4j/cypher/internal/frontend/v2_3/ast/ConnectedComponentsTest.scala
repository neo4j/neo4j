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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ConnectedComponentsTest extends CypherFunSuite {
  import connectedComponents._

  test("(a)->(b), (c)->(d) has two connected components") {
    val disconnected = connectedComponents(Vector(
      ComponentPart(identifier("a"), identifier("b")),
      ComponentPart(identifier("c"), identifier("d"))))

    disconnected should equal(Vector(
      ConnectedComponent(ComponentPart(identifier("a"), identifier("b"))),
        ConnectedComponent(ComponentPart(identifier("c"), identifier("d")))
      ))
  }

  test("(a)->(b)->(c) does contain one connected component") {
    val disconnected = connectedComponents(Vector(
      ComponentPart(identifier("a"), identifier("b")),
      ComponentPart(identifier("b"), identifier("c"))))

    disconnected should equal(Vector(
      ConnectedComponent(ComponentPart(identifier("a"), identifier("b")),
        ComponentPart(identifier("b"), identifier("c")))))
  }

  test("(a)->(b)->(c)->(d) does only contain one component") {
    val disconnected = connectedComponents(Vector(
      ComponentPart(identifier("a"), identifier("b")),
      ComponentPart(identifier("b"), identifier("c")),
      ComponentPart(identifier("c"), identifier("d"))
    ))

    disconnected shouldBe Vector(ConnectedComponent(
      ComponentPart(identifier("a"), identifier("b")),
      ComponentPart(identifier("b"), identifier("c")),
      ComponentPart(identifier("c"), identifier("d")))
    )
  }

  test("(a)->(b)->(c)-(a) contains one component ") {
    val disconnected = connectedComponents(Vector
    (
      ComponentPart(identifier("a"), identifier("b")),
      ComponentPart(identifier("b"), identifier("c")),
      ComponentPart(identifier("c"), identifier("a"))
    ))

    disconnected shouldBe Vector(ConnectedComponent(
      ComponentPart(identifier("a"), identifier("b")),
      ComponentPart(identifier("b"), identifier("c")),
      ComponentPart(identifier("c"), identifier("a"))
    ))
  }

  private def identifier(name: String): Identifier = Identifier(name)(null)
}
