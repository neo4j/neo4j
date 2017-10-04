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

import org.neo4j.cypher._

class SerializationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  // serialization of deleted entities

  test("deleted nodes should be returned marked as such") {
    createNode()

    val query = "MATCH (n) DELETE n RETURN n"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should include("Node[0]{deleted}")
    }
  }

  test("non-deleted nodes should be returned as normal") {
    createNode()

    val query = "MATCH (n) RETURN n"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should not include "deleted"
    }
  }

  test("non-deleted relationships should be returned as normal") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH ()-[r]->() RETURN r"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should not include "deleted"
    }
  }

  test("deleted relationships should be returned marked as such") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH ()-[r]->() DELETE r RETURN r"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should include(":T[0]{deleted}")
    }
  }

  test("returning everything when including deleted entities should work") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH (a)-[r]->(b) DELETE a, r, b RETURN *"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should include(":T[0]{deleted}")
      result.dumpToString() should include("Node[0]{deleted}")
      result.dumpToString() should include("Node[1]{deleted}")
    }
  }

  test("returning a deleted path") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH p=(a)-[r]->(b) DELETE p RETURN p"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should include(":T[0]{deleted}")
      result.dumpToString() should include("Node[0]{deleted}")
      result.dumpToString() should include("Node[1]{deleted}")
    }
  }

  test("returning a deleted path with deleted node") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH p=(a)-[r]->(b) DELETE a, r RETURN p"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should include(":T[0]{deleted}")
      result.dumpToString() should include("Node[0]{deleted}")
      result.dumpToString() should not include("Node[1]{deleted}")
    }
  }

  test("returning a deleted path with deleted relationship") {
    relate(createNode(), createNode(), "T")

    val query = "MATCH p=(a)-[r]->(b) DELETE r RETURN p"

    graph.inTx {
      val result = execute(query)

      result.dumpToString() should include(":T[0]{deleted}")
      result.dumpToString() should not include("Node[0]{deleted}")
      result.dumpToString() should not include("Node[1]{deleted}")
    }
  }

}
