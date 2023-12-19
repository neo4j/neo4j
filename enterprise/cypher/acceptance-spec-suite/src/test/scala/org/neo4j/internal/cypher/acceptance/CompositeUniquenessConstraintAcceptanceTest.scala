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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Versions.{Default, V3_3, V3_4}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class CompositeUniquenessConstraintAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should be able to create and remove single property uniqueness constraint") {

    val testconfiguration = Configs.Procs
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
    TestConfiguration(Versions(V3_4, V3_3, Default), Planners(Planners.Default, Planners.Cost), Runtimes.all)

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
    failWithError(singlePropertyUniquenessFailConf + Configs.Procs,
      "DROP CONSTRAINT ON (n:Person) ASSERT (n.firstname,n.lastname) IS UNIQUE",
      List("Only single property uniqueness constraints are supported"))

    // Then
    graph should not(haveConstraints("UNIQUENESS:Person(firstname,lastname)"))
  }
}
