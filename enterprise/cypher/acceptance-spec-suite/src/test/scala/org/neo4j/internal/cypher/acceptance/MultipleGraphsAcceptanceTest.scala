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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

import scala.language.postfixOps

class MultipleGraphsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  val configs = TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.ProcedureOrSchema, Runtimes.Interpreted, Runtimes.Slotted)) +
    TestConfiguration(Versions.V3_3, Planners.Cost, Runtimes(Runtimes.CompiledBytecode, Runtimes.CompiledSource))
  val expectedException = "Projecting and returning graphs is not available in this implementation of Cypher due to lack of support for multiple graphs."

  test("from graph") {
    val query = "FROM GRAPH AT 'graph://url' AS test MATCH (a)-->() RETURN a"
    failWithError(configs, query, List(expectedException))
  }

  test("into graph") {
    val query = "MATCH (a)--() INTO GRAPH AT 'graph://url' AS test CREATE (a)-->(b:B) RETURN b"
    failWithError(configs, query, List(expectedException))
  }

  test("return named graph") {
    val query = "WITH $param AS foo MATCH ()--() RETURN 1 GRAPHS foo"
    failWithError(configs, query, List(expectedException))
  }

  test("project a graph") {
    val query = "WITH 1 AS a GRAPH foo RETURN 1"
    failWithError(configs, query, List(expectedException))
  }
}
