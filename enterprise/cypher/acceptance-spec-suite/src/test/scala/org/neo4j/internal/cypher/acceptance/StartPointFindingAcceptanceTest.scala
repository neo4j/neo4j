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
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs}

class StartPointFindingAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Not TCK material below; use of id() or index-related

  test("Seek node by id given on the left") {
    createNode("a")
    val node = createNode("b")

    val result =executeWith(Configs.All,
      s"match (n) where ${node.getId} = id(n) return n")
    result.columnAs[Node]("n").toList should equal(List(node))

  }

  test("Seek node by id given on the right") {
    createNode("a")
    val node = createNode("b")

    val result = executeWith(Configs.All,
      s"match (n) where id(n) = ${node.getId} return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }

  test("Seek node by id with multiple values") {
    createNode("a")
    val n1= createNode("b")
    val n2 = createNode("c")

    val result = executeWith(Configs.Interpreted, s"match (n) where id(n) IN [${n1.getId}, ${n2.getId}] return n")
    result.columnAs("n").toList should equal(Seq(n1, n2))
  }

  test("Can use both label scan (left) and node by id (right) when there are no indices") {
    createLabeledNode("Person")
    val node = createLabeledNode("Person")

    val result = executeWith(Configs.All, s"match (n) where n:Person and ${node.getId} = id(n) return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }

  test("Can use both label scan (right) and node by id (left) when there are no indices") {
    createLabeledNode("Person")
    val node = createLabeledNode("Person")

    val result = executeWith(Configs.All, s"match (n) where ${node.getId} = id(n) and n:Person return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }

  test("Seek relationship by id given on the left") {
    val rel = relate(createNode("a"), createNode("b"))

    val result = executeWith(Configs.Interpreted, s"match ()-[r]->() where ${rel.getId} = id(r) return r")
    result.columnAs[Node]("r").toList should equal(List(rel))
  }

  test("Seek relationship by id given on the right") {
    val rel = relate(createNode("a"), createNode("b"))

    val result = executeWith(Configs.Interpreted, s"match ()-[r]->() where id(r) = ${rel.getId} return r")
    result.columnAs[Node]("r").toList should equal(List(rel))
  }

  test("Seek relationship by id with multiple values") {
    relate(createNode("x"), createNode("y"))
    val rel1 = relate(createNode("a"), createNode("b"))
    val rel2 = relate(createNode("c"), createNode("d"))

    val result = executeWith(Configs.Interpreted, s"match ()-[r]->() where id(r) IN [${rel1.getId}, ${rel2.getId}] return r")
    result.columnAs("r").toList should equal(Seq(rel1, rel2))
  }

  test("Seek relationship by id with no direction") {
    val a = createNode("x")
    val b = createNode("x")
    val r = relate(a, b)

    val result = executeWith(Configs.Interpreted, s"match (a)-[r]-(b) where id(r) = ${r.getId} return a,r,b")
    result.toSet should equal(Set(
      Map("r" -> r, "a" -> a, "b" -> b),
      Map("r" -> r, "a" -> b, "b" -> a)))
  }

  test("Seek relationship by id and unwind") {
    val a = createNode("x")
    val b = createNode("x")
    val r = relate(a, b)

    val result = executeWith(Configs.Interpreted, s"PROFILE UNWIND [${r.getId}] as rId match (a)-[r]->(b) where id(r) = rId return a,r,b",
      planComparisonStrategy = ComparePlansWithAssertion(_.toString should include("RelationshipById"), expectPlansToFail = Configs.AllRulePlanners))

    result.toList should equal(List(Map("r" -> r, "a" -> a, "b" -> b)))
  }

  test("Seek relationship by id with type that is not matching") {
    val r = relate(createNode("x"), createNode("y"), "FOO")

    val result = executeWith(Configs.Interpreted, s"match ()-[r:BAR]-() where id(r) = ${r.getId} return r")
    result.toList shouldBe empty
  }

  test("Scan index with property given in where") {
    createLabeledNode("Person")
    graph.createIndex("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    val result = executeWith(Configs.All, s"match (n:Person) where n.prop = 42 return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }

  test("Scan index with property given in node pattern") {
    createLabeledNode("Person")
    graph.createIndex("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    val result = executeWith(Configs.All, s"match (n:Person {prop: 42}) return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }

  test("Seek index with property given in where") {
    createLabeledNode("Person")
    graph.createConstraint("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    val result = executeWith(Configs.All, s"match (n:Person) where n.prop = 42 return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }

  test("Seek index with property given in node pattern") {
    createLabeledNode("Person")
    graph.createConstraint("Person", "prop")

    val node = createLabeledNode(Map("prop" -> 42), "Person")
    val result = executeWith(Configs.All, s"match (n:Person {prop: 42}) return n")
    result.columnAs[Node]("n").toList should equal(List(node))
  }
}

