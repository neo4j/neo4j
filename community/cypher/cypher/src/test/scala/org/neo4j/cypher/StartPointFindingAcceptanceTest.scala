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
package org.neo4j.cypher

import org.neo4j.graphdb.Node

class StartPointFindingAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("Scan all nodes") {
    val nodes = Set(createNode("a"), createNode("b"), createNode("c"))

    executeWithAllPlanners("match n return n").columnAs[Node]("n").toSet should equal(nodes)
  }

  test("Scan labeled node") {
    createNode("a")
    createLabeledNode("Person")
    val animals = Set(createLabeledNode("Animal"), createLabeledNode("Animal"))

    executeWithAllPlanners("match (n:Animal) return n").columnAs[Node]("n").toSet should equal(animals)
  }

  test("Seek node by id given on the left") {
    createNode("a")
    val node = createNode("b")

    executeScalarWithAllPlanners[Node](s"match n where ${node.getId} = id(n) return n") should equal(node)
  }

  test("Seek node by id given on the right") {
    createNode("a")
    val node = createNode("b")

    executeScalarWithAllPlanners[Node](s"match n where id(n) = ${node.getId} return n") should equal(node)
  }

  test("Seek node by id with multiple values") {
    createNode("a")
    val n1= createNode("b")
    val n2 = createNode("c")

    val result = executeWithAllPlanners(s"match n where id(n) IN [${n1.getId}, ${n2.getId}] return n")
    result.columnAs("n").toList should equal(Seq(n1, n2))
  }

  test("Can use both label scan (left) and node by id (right) when there are no indices") {
    createLabeledNode("Person")
    val node = createLabeledNode("Person")

    executeScalarWithAllPlanners[Node](s"match n where n:Person and ${node.getId} = id(n) return n") should equal(node)
  }

  test("Can use both label scan (right) and node by id (left) when there are no indices") {
    createLabeledNode("Person")
    val node = createLabeledNode("Person")

    executeScalarWithAllPlanners[Node](s"match n where ${node.getId} = id(n) and n:Person return n") should equal(node)
  }

  test("Can find nodes by id and apply a predicate on it") {
    createNode("prop"->1)
    val n = createNode("prop"->2)
    executeScalarWithAllPlanners[Node](s"match n where n.prop = 2 return n") should equal(n)
  }

  test("Seek relationship by id given on the left") {
    val rel = relate(createNode("a"), createNode("b"))

    executeScalarWithAllPlanners[Node](s"match ()-[r]->() where ${rel.getId} = id(r) return r") should equal(rel)
  }

  test("Seek relationship by id given on the right") {
    val rel = relate(createNode("a"), createNode("b"))

    executeScalarWithAllPlanners[Node](s"match ()-[r]->() where id(r) = ${rel.getId} return r") should equal(rel)
  }

  test("Seek relationship by id with multiple values") {
    relate(createNode("x"), createNode("y"))
    val rel1 = relate(createNode("a"), createNode("b"))
    val rel2 = relate(createNode("c"), createNode("d"))

    val result = executeWithAllPlanners(s"match ()-[r]->() where id(r) IN [${rel1.getId}, ${rel2.getId}] return r")
    result.columnAs("r").toList should equal(Seq(rel1, rel2))
  }

  test("Seek relationship by id with no direction") {
    val a = createNode("x")
    val b = createNode("x")
    val r = relate(a, b)

    val result = executeWithAllPlanners(s"match (a)-[r]-(b) where id(r) = ${r.getId} return a,r,b")
    result.toList should equal(List(
      Map("r" -> r, "a" -> a, "b" -> b),
      Map("r" -> r, "a" -> b, "b" -> a)))
  }

  test("Seek relationship by id and unwind") {
    val a = createNode("x")
    val b = createNode("x")
    val r = relate(a, b)

    val result = executeWithAllPlanners(s"PROFILE UNWIND [${r.getId}] as rId match (a)-[r]->(b) where id(r) = rId return a,r,b")

    result.executionPlanDescription().toString should include("RelationshipById")

    result.toList should equal(List(Map("r" -> r, "a" -> a, "b" -> b)))
  }

  test("Seek relationship by id with type that is not matching") {
    val r = relate(createNode("x"), createNode("y"), "FOO")

    val result = executeWithAllPlanners(s"match ()-[r:BAR]-() where id(r) = ${r.getId} return r")
    result.toList shouldBe empty
  }

  test("Scan index with property given in where") {
    createLabeledNode("Person")
    graph.createIndex("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithAllPlanners[Node](s"match (n:Person) where n.prop = 42 return n") should equal(node)
  }

  test("Scan index with property given in node pattern") {
    createLabeledNode("Person")
    graph.createIndex("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithAllPlanners[Node](s"match (n:Person {prop: 42}) return n") should equal(node)
  }

  test("Seek index with property given in where") {
    createLabeledNode("Person")
    graph.createConstraint("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithAllPlanners[Node](s"match (n:Person) where n.prop = 42 return n") should equal(node)
  }

  test("Seek index with property given in node pattern") {
    createLabeledNode("Person")
    graph.createConstraint("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    executeScalarWithAllPlanners[Node](s"match (n:Person {prop: 42}) return n") should equal(node)
  }
}

