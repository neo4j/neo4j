/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.api.exceptions.Status


class PropertyExistenceConstraintAcceptanceTest
  extends ExecutionEngineFunSuite
    with QueryStatisticsTestSupport
    with ListSupport
    with EnterpriseGraphDatabaseTestSupport {

  test("node: should enforce constraints on creation") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    // WHEN
    val e = intercept[ConstraintValidationException](execute("create (node:Label1)"))

    // THEN
    e.getMessage should endWith(" with label `Label1` must have the property `key1`")
  }

  test("relationship: should enforce constraints on creation") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    // WHEN
    val e = intercept[ConstraintValidationException](execute("create (p1:Person)-[:KNOWS]->(p2:Person)"))

    // THEN
    e.getMessage should endWith("with type `KNOWS` must have the property `since`")
  }

  test("node: should enforce on removing property") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    // WHEN
    execute("create (node1:Label1 {key1:'value1'})")

    // THEN
    intercept[ConstraintValidationException](execute("match (node:Label1) remove node.key1"))
  }

  test("relationship: should enforce on removing property") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    // WHEN
    execute("create (p1:Person)-[:KNOWS {since: 'yesterday'}]->(p2:Person)")

    // THEN
    intercept[ConstraintValidationException](execute("match (p1:Person)-[r:KNOWS]->(p2:Person) remove r.since"))
  }

  test("node: should enforce on setting property to null") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    // WHEN
    execute("create ( node1:Label1 {key1:'value1' } )")

    //THEN
    intercept[ConstraintValidationException](execute("match (node:Label1) set node.key1 = null"))
  }

  test("relationship: should enforce on setting property to null") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    // WHEN
    execute("create (p1:Person)-[:KNOWS {since: 'yesterday'}]->(p2:Person)")

    //THEN
    intercept[ConstraintValidationException](execute("match (p1:Person)-[r:KNOWS]->(p2:Person) set r.since = null"))
  }

  test("node: should allow to break constraint within statement") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    // WHEN
    val res = execute("create (node:Label1) set node.key1 = 'foo' return node")

    //THEN
    res.toList should have size 1
  }

  test("relationship: should allow to break constraint within statement") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    // WHEN
    val res = execute("create (p1:Person)-[r:KNOWS]->(p2:Person) set r.since = 'yesterday' return r")

    //THEN
    res.toList should have size 1
  }

  test("node: should allow creation of non-conflicting data") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    // WHEN
    execute("create (node {key1:'value1'} )")
    execute("create (node:Label2)")
    execute("create (node:Label1 { key1:'value1'})")
    execute("create (node:Label1 { key1:'value1'})")

    // THEN
    numberOfNodes shouldBe 4
  }

  test("relationship: should allow creation of non-conflicting data") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    // WHEN
    execute("create (p1:Person {name: 'foo'})-[r:KNOWS {since: 'today'}]->(p2:Person {name: 'bar'})")
    execute("create (p1:Person)-[:FOLLOWS]->(p2:Person)")
    execute("create (p:Person {name: 'Bob'})<-[:KNOWS {since: '2010'}]-(a:Animal {name: 'gädda'})")

    // THEN
    numberOfRelationships shouldBe 3
  }

  test("node: should fail to create constraint when existing data violates it") {
    // GIVEN
    execute("create (node:Label1)")

    // WHEN
    val e = intercept[CypherExecutionException](execute("create constraint on (node:Label1) assert exists(node.key1)"))

    //THEN
    e.status should equal(Status.Schema.ConstraintCreationFailed)
  }

  test("relationship: should fail to create constraint when existing data violates it") {
    // GIVEN
    execute("create (p1:Person)-[:KNOWS]->(p2:Person)")

    // WHEN
    val e = intercept[CypherExecutionException](execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)"))

    //THEN
    e.status should equal(Status.Schema.ConstraintCreationFailed)
  }

  test("node: should drop constraint") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    intercept[ConstraintValidationException](execute("create (node:Label1)"))
    numberOfNodes shouldBe 0

    // WHEN
    execute("drop constraint on (node:Label1) assert exists(node.key1)")
    execute("create (node:Label1)")

    // THEN
    numberOfNodes shouldBe 1
  }

  test("relationship: should drop constraint") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    intercept[ConstraintValidationException](execute("create (p1:Person)-[:KNOWS]->(p2:Person)"))
    numberOfRelationships shouldBe 0

    // WHEN
    execute("drop constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")
    execute("create (p1:Person)-[:KNOWS]->(p2:Person)")

    // THEN
    numberOfRelationships shouldBe 1
  }

  test("should not use countStore short cut when no constraint exist") {
    val plan = execute("MATCH (n:X) RETURN count(n.foo)")

    plan shouldNot use("NodeCountFromCountStore")
  }

  test("should use countStore short cut when constraint exist") {
    execute("CREATE CONSTRAINT ON (n:X) ASSERT EXISTS(n.foo)")

    val result = execute("MATCH (n:X) RETURN count(n.foo)")

    result should use("NodeCountFromCountStore")
  }

  private def numberOfNodes = executeScalar[Long]("match (n) return count(n)")

  private def numberOfRelationships = executeScalar[Long]("match ()-[r]->() return count(r)")
}


