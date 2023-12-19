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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.helpers.{NodeKeyConstraintCreator, UniquenessConstraintCreator}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs}

class UniqueIndexAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  Seq(UniquenessConstraintCreator, NodeKeyConstraintCreator).foreach { constraintCreator =>

    test(s"$constraintCreator: should be able to use unique index hints on IN expressions") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")
      //THEN
      result.toList should equal(List(Map("n" -> jake)))
    }

    test(s"$constraintCreator: should be able to use unique index on IN collections with duplicates") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

      //THEN
      result.toList should equal(List(Map("n" -> jake)))
    }

    test(s"$constraintCreator: should be able to use unique index on IN a null value") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

      //THEN
      result.toList should equal(List())
    }

    test(s"$constraintCreator: should be able to use index unique index on IN a collection parameter") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n", params = Map("coll" -> List("Jacob")))

      //THEN
      result.toList should equal(List(Map("n" -> jake)))
    }

    test(s"$constraintCreator: should not use locking index for read only query") {
      //GIVEN
      val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
      val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
      relate(andres, createNode())
      relate(jake, createNode())

      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = executeWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n",
        planComparisonStrategy = ComparePlansWithAssertion((plan) => {
          //THEN
          plan should useOperators("NodeUniqueIndexSeek")
          plan shouldNot useOperators("NodeUniqueIndexSeek(Locking)")
        }, Configs.AllRulePlanners),
        params = Map("coll" -> List("Jacob")))
    }

     test(s"$constraintCreator: should use locking unique index for merge node queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      executeWith(Configs.Interpreted - Configs.Cost2_3, "MERGE (n:Person {name: 'Andres'}) RETURN n.name",
        planComparisonStrategy = ComparePlansWithAssertion((plan) => {
          //THEN
          plan shouldNot useOperators("NodeIndexSeek")
          plan should useOperators("NodeUniqueIndexSeek(Locking)")
        }, Configs.AllRulePlanners))
    }

    test(s"$constraintCreator: should use locking unique index for merge relationship queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      executeWith(Configs.Interpreted - Configs.Cost2_3,
        "PROFILE MATCH (n:Person {name: 'Andres'}) MERGE (n)-[:KNOWS]->(m:Person {name: 'Maria'}) RETURN n.name",
        planComparisonStrategy = ComparePlansWithAssertion((plan) => {
          // THEN
          plan shouldNot useOperators("NodeIndexSeek")
          plan shouldNot useOperators("NodeByLabelScan")
          plan should useOperators("NodeUniqueIndexSeek(Locking)")
        }, Configs.AllRulePlanners + Configs.Cost3_1))
    }

    test(s"$constraintCreator: should use locking unique index for mixed read write queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      val query = "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} SET n:Foo RETURN n.name"
      //WHEN
      executeWith(Configs.Interpreted - Configs.Cost2_3, query, params = Map("coll" -> List("Jacob")),
        planComparisonStrategy = ComparePlansWithAssertion((plan) => {
          //THEN
          plan shouldNot useOperators("NodeIndexSeek")
          plan should useOperators("NodeUniqueIndexSeek(Locking)")
        }, Configs.AllRulePlanners))
    }
  }

  test("should handle null with locking unique index seeks") {
    //GIVEN
    createLabeledNode("Person")
    UniquenessConstraintCreator.createConstraint(graph, "Person", "name")
    graph should not(haveConstraints(s"${UniquenessConstraintCreator.other.typeName}:Person(name)"))
    graph should haveConstraints(s"${UniquenessConstraintCreator.typeName}:Person(name)")

    val query = "MATCH (n:Person) WHERE n.name = null SET n:FOO"
    //WHEN
    executeWith(Configs.Interpreted - Configs.Cost2_3, query, planComparisonStrategy = ComparePlansWithAssertion((plan) => {
      //THEN
      plan shouldNot useOperators("NodeIndexSeek")
      plan shouldNot useOperators("NodeByLabelScan")
      plan should useOperators("NodeUniqueIndexSeek(Locking)")
    }, Configs.AllRulePlanners + Configs.Cost3_1))
  }
}
