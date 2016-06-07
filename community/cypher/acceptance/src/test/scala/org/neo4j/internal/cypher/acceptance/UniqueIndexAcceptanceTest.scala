/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class UniqueIndexAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should be able to use unique index hints on IN expressions") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createConstraint("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should be able to use unique index on IN collections with duplicates") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createConstraint("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should be able to use unique index on IN a null value") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createConstraint("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

    //THEN
    result.toList should equal (List())
  }

  test("should be able to use index unique index on IN a collection parameter") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createConstraint("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n","coll"->List("Jacob"))

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should not use locking index for read only query") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createConstraint("Person", "name")

    //WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n","coll"->List("Jacob"))

    //THEN
    result should use("NodeUniqueIndexSeek")
    result shouldNot use("NodeUniqueIndexSeek(Locking)")
  }

  test("should use locking unique index for merge queries") {
    //GIVEN
    createLabeledNode(Map("name" -> "Andres"), "Person")
    graph.createConstraint("Person", "name")

    //WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (n:Person {name: 'Andres'}) RETURN n.name")

    //THEN
    result shouldNot use("NodeIndexSeek")
    result should use("NodeUniqueIndexSeek(Locking)")
  }

  test("should use locking unique index for mixed read write queries") {
    //GIVEN
    createLabeledNode(Map("name" -> "Andres"), "Person")
    graph.createConstraint("Person", "name")

    //WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} SET n:Foo RETURN n.name","coll"->List("Jacob"))

    //THEN
    result shouldNot use("NodeIndexSeek")
    result should use("NodeUniqueIndexSeek(Locking)")
  }
}
