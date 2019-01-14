/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class ShortestPathEdgeCasesAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("GH #5803 query should work with shortest path") {
    def createTestGraph() = {
      // First create some background graph to get the stats right
      (0 to 16).map(_ => createNode()).sliding(2).foreach {
        case Seq(n1, n2) => relate(n1, n2)
      }
      // Now create the specific subgraph used for the test
      graph.createIndex("WP", "id")
      val query = """create (_31801:`WP` {`id`:1})
                    |create (_31802:`WP` {`id`:2})
                    |create (_31803:`WP` {`id`:3})
                    |create (_31804:`WP` {`id`:4})
                    |create (_31805:`WP` {`id`:5})
                    |create (_31806:`WP` {`id`:11})
                    |create (_31807:`WP` {`id`:12})
                    |create (_31808:`WP` {`id`:13})
                    |create (_31809:`WP` {`id`:22})
                    |create (_31810:`WP` {`id`:23})
                    |create (_31811:`WP` {`id`:21})
                    |create (_31812:`WP` {`id`:14})
                    |create (_31813:`WP` {`id`:29})
                    |create (_31814:`WP` {`id`:15})
                    |create (_31815:`WP` {`id`:24})
                    |create (_31816:`WP` {`id`:25})
                    |create (_31817:`WP` {`id`:26})
                    |create (_31818:`WP` {`id`:27})
                    |create (_31819:`WP` {`id`:28})
                    |create (_31820:`WP` {`id`:30})
                    |create (_31801)-[:`SE`]->(_31806)
                    |create (_31801)-[:`SE`]->(_31802)
                    |create (_31802)-[:`SE`]->(_31807)
                    |create (_31802)-[:`SE`]->(_31803)
                    |create (_31803)-[:`SE`]->(_31808)
                    |create (_31803)-[:`SE`]->(_31804)
                    |create (_31804)-[:`SE`]->(_31812)
                    |create (_31804)-[:`SE`]->(_31805)
                    |create (_31805)-[:`SE`]->(_31814)
                    |create (_31805)-[:`SE`]->(_31801)
                    |create (_31806)-[:`SE`]->(_31809)
                    |create (_31806)-[:`SE`]->(_31811)
                    |create (_31806)-[:`SE`]->(_31807)
                    |create (_31807)-[:`SE`]->(_31815)
                    |create (_31807)-[:`SE`]->(_31810)
                    |create (_31807)-[:`SE`]->(_31808)
                    |create (_31808)-[:`SE`]->(_31817)
                    |create (_31808)-[:`SE`]->(_31816)
                    |create (_31808)-[:`SE`]->(_31812)
                    |create (_31809)-[:`SE`]->(_31811)
                    |create (_31810)-[:`SE`]->(_31809)
                    |create (_31811)-[:`SE`]->(_31820)
                    |create (_31812)-[:`SE`]->(_31819)
                    |create (_31812)-[:`SE`]->(_31818)
                    |create (_31812)-[:`SE`]->(_31814)
                    |create (_31813)-[:`SE`]->(_31819)
                    |create (_31814)-[:`SE`]->(_31820)
                    |create (_31814)-[:`SE`]->(_31813)
                    |create (_31814)-[:`SE`]->(_31806)
                    |create (_31815)-[:`SE`]->(_31810)
                    |create (_31816)-[:`SE`]->(_31815)
                    |create (_31817)-[:`SE`]->(_31816)
                    |create (_31818)-[:`SE`]->(_31817)
                    |create (_31819)-[:`SE`]->(_31818)
                    |create (_31820)-[:`SE`]->(_31813)""".stripMargin
      graph.execute(query)
    }

    createTestGraph()
    val query = """WITH [1,3,26,14] as wps
                  |UNWIND wps AS wpstartid
                  |UNWIND wps AS wpendid
                  |WITH wpstartid, wpendid, wps
                  |WHERE wpstartid<wpendid
                  |MATCH (wpstart {id:wpstartid})
                  |MATCH (wpend {id:wpendid})
                  |MATCH p=shortestPath((wpstart)-[*..10]-(wpend))
                  |WHERE ALL(id IN wps WHERE id IN EXTRACT(n IN nodes(p) | n.id))
                  |WITH p, size(nodes(p)) as length order by length limit 1
                  |RETURN size(nodes(p)) as size""".stripMargin
    val results = executeWith(Configs.Interpreted, query)
    results.toList should equal(List(Map("size" -> 7)))
  }

  test("Predicate should associate with correct shortestPath in complex query") {
    // Given query with two shortestPath expressions and a predicate that depends on both paths
    val query =
      """
        |MATCH (r1:Road) WHERE r1.latitude = 51.357397146246264 AND r1.longitude = -0.20153965352074504
        |MATCH (r2:Road) WHERE r2.latitude = 51.36272835382321 AND r2.longitude = -0.16836400394638354
        |MATCH path = shortestpath((r1)-[:CONNECTS*]-(r2))
        |WITH r1, r2, path
        |MATCH returnPath = shortestpath((r2)-[:CONNECTS*]-(r1))
        |WHERE none(rel in relationships(returnPath) where rel in relationships(path))
        |RETURN returnPath
      """.stripMargin

    // When executing this query
    val result = executeUsingCostPlannerOnly(query)
    val shortestPathOperations = result.executionPlanDescription().find("ShortestPath")
    val (shortestPathWithPredicate, shortestPathWithoutPredicate) = shortestPathOperations.partition { op =>
      op.variables.contains("returnPath")
    }

    // Then the predicate should only be associated with the correct shortestPath pattern
    shortestPathWithPredicate.size should be(1)
    shortestPathWithoutPredicate.size should be(1)
    val withPredicateOther = shortestPathWithPredicate.head.arguments.map(_.toString).mkString(", ")
    val withoutPredicateOther = shortestPathWithoutPredicate.head.arguments.map(_.toString).mkString(", ")
    withPredicateOther should include("relationships")
    withoutPredicateOther should not include "relationships"
  }

  def executeUsingCostPlannerOnly(query: String) =
    RewindableExecutionResult(eengine.execute(s"CYPHER planner=COST $query", Map.empty[String, Any]))
}
