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
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Configs
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport

import scala.language.postfixOps

class UnsupportedFeaturesAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  test("from graph") {
    val query = "FROM GRAPH foo.bar MATCH (a)-->() RETURN a"
    failWithError(Configs.Version3_5, query, List("The `FROM GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("return graph") {
    val query = "WITH $param AS foo MATCH ()--() RETURN GRAPH"
    failWithError(Configs.Version3_5, query, List("The `RETURN GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("construct graph") {
    val query = "MATCH (a) CONSTRUCT ON foo.bar CLONE a CREATE (a)-[:T {prop: a.prop}]->(:X) RETURN 1 AS a"
    failWithError(Configs.Version3_5, query, List("The `CONSTRUCT` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("create graph") {
    val query = "CATALOG CREATE GRAPH foo { RETURN GRAPH }"
    failWithError(Configs.Version3_5, query, List("The `CATALOG CREATE GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("delete graph") {
    val query = "CATALOG DROP GRAPH foo"
    failWithError(Configs.Version3_5, query, List("The `CATALOG DROP GRAPH` clause is not available in this implementation of Cypher due to lack of support for multiple graphs."))
  }

  test("equivalence operator") {
    val query = "RETURN 1 ~ 2"
    failWithError(Configs.Version3_5, query, List("`~` (equivalence) is a Cypher 10 feature and is not available in this implementation of Cypher."))
  }
}
