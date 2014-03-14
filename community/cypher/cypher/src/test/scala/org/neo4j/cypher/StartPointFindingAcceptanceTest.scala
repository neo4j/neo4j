/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.graphdb.Node

class StartPointFindingAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Scan all nodes") {
    val nodes = Set(createNode("a"), createNode("b"), createNode("c"))

    executeWithNewPlanner("match n return n").columnAs[Node]("n").toSet should equal(nodes)
  }

  test("Scan labeled node") {
    createNode("a")
    createLabeledNode("Person")
    val animals = Set(createLabeledNode("Animal"), createLabeledNode("Animal"))

    executeWithNewPlanner("match (n:Animal) return n").columnAs[Node]("n").toSet should equal(animals)
  }

  test("Seek node by id given on the left") {
    createNode("a")
    val node = createNode("b")

    executeScalarWithNewPlanner[Node](s"match n where ${node.getId} = id(n) return n") should equal(node)
  }

  test("Seek node by id given on the right") {
    createNode("a")
    val node = createNode("b")

    executeScalarWithNewPlanner[Node](s"match n where id(n) = ${node.getId} return n") should equal(node)
  }

  // 2014-03-13 - Davide: this is not done by NodeByIdSeek so it is not support by Ronja, we need Filter Pipe for this
  ignore("Can use both label scan (left) and node by id (right)") {
    createLabeledNode("Person")
    val node = createLabeledNode("Person")

    executeScalarWithNewPlanner[Node](s"match n where n:Person and ${node.getId} = id(n) return n") should equal(node)
  }

  // 2014-03-13 - Davide: this is not done by NodeByIdSeek so it is not support by Ronja, we need Filter Pipe for this
  ignore("Can use both label scan (right) and node by id (left)") {
    createLabeledNode("Person")
    val node = createLabeledNode("Person")

    executeScalarWithNewPlanner[Node](s"match n where ${node.getId} = id(n) and n:Person return n") should equal(node)
  }

  // 2014-03-12 SP: Enable once Ronja accepts relationship patterns
  ignore("Seek relationship by id given on the left") {
    val rel = relate(createNode("a"), createNode("b"))

    executeScalarWithNewPlanner[Node](s"match ()-[r]->() where ${rel.getId} = id(r) return r") should equal(rel)
  }

  // 2014-03-12 SP: Enable once Ronja accepts relationship patterns
  ignore("Seek relationship by id given on the right") {
    val rel = relate(createNode("a"), createNode("b"))

    executeScalarWithNewPlanner[Node](s"match ()-[r]->() where id(r) = ${rel.getId} return r") should equal(rel)
  }

  test("Scan index with property given in where") {
    createLabeledNode("Person")
    graph.createIndex("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithNewPlanner[Node](s"match (n:Person) where n.prop = 42 return n") should equal(node)
  }

  test("Scan index with property given in node pattern") {
    createLabeledNode("Person")
    graph.createIndex("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithNewPlanner[Node](s"match (n:Person {prop: 42}) return n") should equal(node)
  }

  test("Seek index with property given in where") {
    createLabeledNode("Person")
    graph.createConstraint("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithNewPlanner[Node](s"match (n:Person) where n.prop = 42 return n") should equal(node)
  }

  test("Seek index with property given in node pattern") {
    createLabeledNode("Person")
    graph.createConstraint("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithNewPlanner[Node](s"match (n:Person {prop: 42}) return n") should equal(node)
  }
}

