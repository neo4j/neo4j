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

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine

class StoreIntegrationAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  // Not TCK material
  test("should not create labels id when trying to delete non-existing labels") {
    createNode()

    val result = executeWith(Configs.UpdateConf, "MATCH (n) REMOVE n:BAR RETURN id(n) AS id")

    assertStats(result, labelsRemoved = 0)
    result.toList should equal(List(Map("id" -> 0)))

    graph.inTx {
      graph.getDependencyResolver.resolveDependency(classOf[RecordStorageEngine]).testAccessNeoStores().getLabelTokenStore.getHighId should equal(0)
    }
  }
}
