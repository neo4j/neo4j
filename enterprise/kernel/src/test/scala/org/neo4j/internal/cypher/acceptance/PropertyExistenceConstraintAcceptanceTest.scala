/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.kernel.api.exceptions.Status

class PropertyExistenceConstraintAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
with CollectionSupport with EnterpriseGraphDatabaseTestSupport{

  test("node: should enforce constraints on creation") {
    // GIVEN
    execute("create constraint on (node:Label1) assert exists(node.key1)")

    // WHEN
    val e = intercept[ConstraintValidationException](execute("create (node:Label1)"))

    // THEN
    e.getMessage should endWith("with label \"Label1\" must have the property \"key1\" due to a constraint")
  }

  test("relationship: should enforce constraints on creation") {
    // GIVEN
    execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)")

    // WHEN
    val e = intercept[ConstraintValidationException](execute("create (p1:Person)-[:KNOWS]->(p2:Person)"))

    // THEN
    e.getMessage should endWith("with type \"KNOWS\" must have the property \"since\" due to a constraint")
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
    e.status should equal(Status.Schema.ConstraintCreationFailure)
  }

  test("relationship: should fail to create constraint when existing data violates it") {
    // GIVEN
    execute("create (p1:Person)-[:KNOWS]->(p2:Person)")

    // WHEN
    val e = intercept[CypherExecutionException](execute("create constraint on ()-[rel:KNOWS]-() assert exists(rel.since)"))

    //THEN
    e.status should equal(Status.Schema.ConstraintCreationFailure)
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

  private def numberOfNodes = executeScalar[Long]("match n return count(n)")

  private def numberOfRelationships = executeScalar[Long]("match ()-[r]->() return count(r)")
}


