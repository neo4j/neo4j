/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.hamcrest.CoreMatchers.containsString
import org.junit.Assert.assertThat
import org.neo4j.cypher._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper._
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Versions.{Default, V3_2, V3_3}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

import scala.collection.JavaConverters._
import scala.collection.Map

class CompositeUniquenessConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  override protected def createGraphDatabase(config: Map[Setting[_], String] = databaseConfig()): GraphDatabaseCypherService = {
    new GraphDatabaseCypherService(new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(config.asJava))
  }

  test("should be able to create and remove single property uniqueness constraint") {

    val testconfiguration = TestConfiguration(Versions(V3_2, V3_3, Default), Planners.Default, Runtimes.ProcedureOrSchema)
    // When
    executeWith(testconfiguration, "CREATE CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should haveConstraints("UNIQUENESS:Person(email)")

    // When
    executeWith(testconfiguration, "DROP CONSTRAINT ON (n:Person) ASSERT (n.email) IS UNIQUE")

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(email)"))
  }

  val singlePropertyUniquenessFailConf =
    TestConfiguration(Versions(V3_2, V3_3, Default), Planners(Planners.Default, Planners.Cost), Runtimes.all)

  test("should fail to to create composite uniqueness constraints") {
    // When

    failWithError(singlePropertyUniquenessFailConf,
      "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      List("Only single property uniqueness constraints are supported"))

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }

  test("should fail to to drop composite uniqueness constraints") {
    // When
    failWithError(singlePropertyUniquenessFailConf + TestScenario(Versions.Default, Planners.Default, Runtimes.ProcedureOrSchema),
      "DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      List("Only single property uniqueness constraints are supported"))

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }
}
