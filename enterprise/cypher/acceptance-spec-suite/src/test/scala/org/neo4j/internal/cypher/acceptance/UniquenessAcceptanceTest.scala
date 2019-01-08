/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.Path

class UniquenessAcceptanceTest extends ExecutionEngineFunSuite {

  test("should not reuse the relationship that has just been traversed") {
    // Given
    relate(createNode("Me"), createNode("Bob"))

    // When
    val result = execute("MATCH (a)-->()-->(b) WHERE a.name = 'Me' RETURN b.name")

    // Then
    result.toList shouldBe empty
  }

  test("should not reuse a relationship that was used earlier") {
    // Given a graph: n1-->n2, n2-->n2
    val n1 = createNode("start")
    val n2 = createNode()
    relate(n1, n2)
    relate(n2, n2)

    // When
    val result = execute("match (a)--(b)-->(c)--(d) where a.name = 'start' return d")

    // Then
    result.toList shouldBe empty
  }

  test("should reuse relationships that were used in a different clause") {
    // Given
    // leaf1-->parent
    // leaf2-->parent
    val leaf1 = createNode("leaf1")
    val leaf2 = createNode("leaf2")
    val parent = createNode("parent")
    relate(leaf1, parent)
    relate(leaf2, parent)

    // When
    val result = execute("MATCH (x)-->(parent) WHERE x.name = 'leaf1' WITH parent MATCH (leaf)-->(parent) RETURN leaf")

    // Then
    result should have size 2
  }

  test("should reuse relationships even though they are in context but not used in a pattern") {
    // Given
    // leaf1-->parent
    // leaf2-->parent
    val leaf1 = createNode("leaf1")
    val leaf2 = createNode("leaf2")
    val parent = createNode("parent")
    relate(leaf1, parent)
    relate(leaf2, parent)

    // When
    val result = execute("MATCH (x)-[r]->(parent) WHERE x.name = 'leaf1' WITH r, parent MATCH (leaf)-->(parent) RETURN r, leaf")

    // Then
    result should have size 2
  }

  test("should consider uniqueness when combining simple and variable length pattern in a match") {
      // Given
      val leaf1 = createNode("leaf1")
      val leaf2 = createNode("leaf2")

      relate(leaf1, leaf2) // r1
      relate(leaf2, leaf1) // r2

      // When
      val result = executeScalar[Number](s"MATCH (a)-->()-[*0..4]-(c) WHERE id(a) = ${leaf1.getId} RETURN count(*)")

      // Then find paths: leaf1-[r1]->(leaf2), leaf1-[r1]->(leaf2)<-[r2]-(leaf1)
      result should equal(2)
    }

  test("should consider uniqueness when combining variable and simple length pattern in a match") {

      // Given
      val leaf1 = createNode("leaf1")
      val leaf2 = createNode("leaf2")

      relate(leaf1, leaf2) // r1
      relate(leaf2, leaf1) // r2

      // When
      val result = executeScalar[Number](s"MATCH (a)-[*0..4]-()<--(c) WHERE id(a) = ${leaf1.getId} RETURN count(*)")

      // Then find paths: leaf1-[r1]->(leaf2), leaf1-[r1]->(leaf2)<-[r2]-(leaf1)
      result should equal(2)
    }

  test("should consider uniqueness when combining two variable length patterns in a match") {
      // Given
      val leaf1 = createNode("leaf1")
      val leaf2 = createNode("leaf2")

      relate(leaf1, leaf2) // r1
      relate(leaf2, leaf1) // r2

      // When
      val result = executeScalar[Number](s"MATCH (a)-[*1..4]->()-[*0..5]-(c) WHERE id(a) = ${leaf1.getId} RETURN count(*)")

      // Then find paths
      // r1
      // r1 >> r2
      // r1-r2
      result should equal(3)
    }

  test("should consider uniqueness when doing cartesian products") {
    // Given
    val r1 = relate(createNode(), createNode())
    val r2 = relate(createNode(), createNode())

      // When
      val result = execute("MATCH p=()-->(), q=()-->() RETURN p, q")

      // Then find paths
      val rels = result.toList.map(row => row("p").asInstanceOf[Path].lastRelationship())

      rels should have size 2
      rels.toSet should equal(Set(r1, r2))
    }
}
