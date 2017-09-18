/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb._

import scala.collection.JavaConverters._

class PathImplTest extends CypherFunSuite with FakeEntityTestSupport {

  val typ = RelationshipType.withName("a")

  test("singleNodeTests") {
    val node = new FakeNode
    val path = new PathImpl(node)

    path.length() should equal(0)
    path.startNode() should equal(node)
    path.endNode() should equal(node)
    path.nodes().asScala.toList should equal(List(node))
    path.relationships().asScala.toList should equal(List())
    path.toSeq should equal(Seq(node))
  }

  test("twoNodesOneRelationship") {
    val nodA = new FakeNode
    val nodB = new FakeNode
    val rel = new FakeRel(nodA, nodB, typ)
    val path = new expressions.PathImpl(nodA, rel, nodB)

    path.length() should equal(1)
    path.startNode() should equal(nodA)
    path.endNode() should equal(nodB)
    path.nodes().asScala.toList should equal(List(nodA, nodB))
    path.relationships().asScala.toList should equal(List(rel))
    path.toSeq should equal(Seq(nodA, rel, nodB))
  }

  test("acceptOnlyProperPaths") {
    val nodA = new FakeNode
    val nodB = new FakeNode
    val rel1 = new FakeRel(nodA, nodB, typ)

    val badPaths = List(
      Seq(nodA, nodB),
      Seq(nodA, rel1),
      Seq(rel1, nodA),
      Seq(rel1)
    )

    badPaths.foreach(p => intercept[IllegalArgumentException](new expressions.PathImpl(p:_*)))
  }

  test("retrieveLastRelationshipOnLongPath") {
    val nodA = new FakeNode
    val nodB = new FakeNode
    val nodC = new FakeNode
    val rel1 = new FakeRel(nodA, nodB, typ)
    val rel2 = new FakeRel(nodB, nodC, typ)
    val path = new PathImpl(nodA, rel1, nodB, rel2, nodC)

    path.lastRelationship() should equal(rel2)
  }
}
