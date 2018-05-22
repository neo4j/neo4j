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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

import scala.language.postfixOps

class MultipleGraphsAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  val configs = Configs.Version3_5 + Configs.Version3_3 + Configs.Procs - Configs.AllRulePlanners
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
