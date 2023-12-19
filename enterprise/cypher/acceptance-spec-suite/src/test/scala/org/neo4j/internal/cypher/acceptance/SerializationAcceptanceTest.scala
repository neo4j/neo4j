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
