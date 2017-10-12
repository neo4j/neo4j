/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher._
import org.neo4j.graphdb._

class OrderByAcceptanceTest extends ExecutionEngineFunSuite {

  test("should order by a list property") {
    graph.execute("MERGE (n {id: 0}) RETURN n")
    graph.execute("MERGE (n {id: 1, prop: []}) RETURN n")

    val exception = intercept[QueryExecutionException] {
      graph.execute("MATCH (n) RETURN n ORDER BY n.prop")
    }
    exception.getMessage should be("Cannot perform ORDER BY on mixed types. Left: [] (String[]); Right: <null>")
  }

}
