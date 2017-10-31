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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node

class ShortestPathEdgeCasesAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

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
                  |RETURN EXTRACT(n IN nodes(p) | n.id) as nodes""".stripMargin
    val results = executeWithCostPlannerOnly(query)
    results.toList should equal(List(Map("nodes" -> List(1,2,3,4,14,13,26))))
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
    withPredicateOther should include("RelationshipFunction")
    withoutPredicateOther should not include "RelationshipFunction"
  }

  def executeUsingCostPlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=COST $query", Map.empty[String, Any]) match {
      case e: ClosingExecutionResult => RewindableExecutionResult(e.inner)
    }

}
