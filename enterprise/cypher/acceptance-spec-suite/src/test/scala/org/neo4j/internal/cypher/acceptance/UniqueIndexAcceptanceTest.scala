/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.helpers.{NodeKeyConstraintCreator, UniquenessConstraintCreator}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

import scala.collection.Map
import scala.collection.JavaConverters._

class UniqueIndexAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  override protected def createGraphDatabase(config: Map[Setting[_], String] = databaseConfig()): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(config.asJava))
  }

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
      val result = executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob'] RETURN n")

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
      val result = executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN ['Jacob','Jacob'] RETURN n")

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
      val result = executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN null RETURN n")

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
      val result = executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n", "coll" -> List("Jacob"))

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
      val result = executeWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} RETURN n", "coll" -> List("Jacob"))

      //THEN
      result should use("NodeUniqueIndexSeek")
      result shouldNot use("NodeUniqueIndexSeek(Locking)")
    }

    test(s"$constraintCreator: should use locking unique index for merge node queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = updateWithCompatibilityAndAssertSimilarPlans("MERGE (n:Person {name: 'Andres'}) RETURN n.name")

      //THEN
      result shouldNot use("NodeIndexSeek")
      result should use("NodeUniqueIndexSeek(Locking)")
    }

    test(s"$constraintCreator: should use locking unique index for merge relationship queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n:Person {name: 'Andres'}) MERGE (n)-[:KNOWS]->(m:Person {name: 'Maria'}) RETURN n.name")

      //THEN
      result shouldNot use("NodeIndexSeek")
      result shouldNot use("NodeByLabelScan")
      result should use("NodeUniqueIndexSeek(Locking)")
    }

    test(s"$constraintCreator: should use locking unique index for mixed read write queries") {
      //GIVEN
      createLabeledNode(Map("name" -> "Andres"), "Person")
      constraintCreator.createConstraint(graph, "Person", "name")
      graph should not(haveConstraints(s"${constraintCreator.other.typeName}:Person(name)"))
      graph should haveConstraints(s"${constraintCreator.typeName}:Person(name)")

      //WHEN
      val result = updateWithCompatibilityAndAssertSimilarPlans("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name IN {coll} SET n:Foo RETURN n.name", "coll" -> List("Jacob"))

      //THEN
      result shouldNot use("NodeIndexSeek")
      result should use("NodeUniqueIndexSeek(Locking)")
    }
  }
}
