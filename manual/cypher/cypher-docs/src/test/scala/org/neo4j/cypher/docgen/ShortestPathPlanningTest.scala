/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.docgen.tooling._
import org.neo4j.graphdb.Path

class ShortestPathPlanningTest extends DocumentingTest {
  override def outputPath = "target/docs/dev/execution-plan-groups/"
  override def doc = new DocBuilder {
    doc("Shortest path planning", "query-shortestpath-planning")
    initQueries(
      """CREATE (charlie:Person {name:'Charlie Sheen'}),
        |       (martin:Person {name: 'Martin Sheen'}),
        |       (michael:Person {name: 'Michael Douglas'}),
        |       (oliver:Person {name: 'Oliver Stone'}),
        |       (rob:Person {name: 'Rob Reiner'}),
        |
        |       (wallStreet:Movie {title: 'Wall Street'}),
        |       (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet),
        |       (martin)-[:ACTED_IN {role: 'Carl Fox'}]->(wallStreet),
        |       (michael)-[:ACTED_IN {role: 'Gordon Gekko'}]->(wallStreet),
        |       (oliver)-[:DIRECTED]->(wallStreet),
        |
        |       (thePresident:Movie {title: 'The American President'}),
        |       (martin)-[:ACTED_IN {role: 'A.J. MacInerney'}]->(thePresident),
        |       (michael)-[:ACTED_IN {role: 'President Andrew Shepherd'}]->(thePresident),
        |       (rob)-[:DIRECTED]->(thePresident)""",
        "CREATE INDEX ON :Person(name)"
    )
    synopsis("Shortest path finding in Cypher and how it is planned.")
    p("""Planning shortest paths in Cypher can lead to different query plans depending on the predicates that need
        |to be evaluated. Internally, Neo4j will use a fast bidirectional breadth-first search algorithm if the
        |predicates can be evaluated whilst searching for the path. Therefore, this fast algorithm will always
        |be certain to return the right answer when there are universal predicates on the path; for example, when
        |searching for the shortest path where all nodes have the `Person` label, or where there are no nodes with
        |a `name` property.""".stripMargin)
    p("""If the predicates need to inspect the whole path before deciding on whether it is valid or not, this fast
        |algorithm cannot be relied on to find the shortest path, and Neo4j may have to resort to using a slower
        |exhaustive depth-first search algorithm to find the path. This means that query plans for shortest path
        |queries with non-universal predicates will include a fallback to running the exhaustive search to find
        |the path should the fast algorithm not succeed. For example, depending on the data, an answer to a shortest
        |path query with existential predicates -- such as the requirement that at least one node contains the property
        |`name='Charlie Sheen'` -- may not be able to be found by the fast algorithm. In this case, Neo4j will fall back to using
        |the exhaustive search to enumerate all paths and potentially return an answer.""".stripMargin)
    p("""The running times of these two algorithms may differ by orders of magnitude, so it is important to ensure
        |that the fast approach is used for time-critical queries.""".stripMargin)
    p("""When the exhaustive search is planned, it is still only executed when the fast algorithm fails to find any
        |matching paths. The fast algorithm is always executed first, since it is possible that it can find a valid
        |path even though that could not be guaranteed at planning time.""".stripMargin)
    section("Shortest path with fast algorithm") {
      query(
        """MATCH (ms:Person {name:'Martin Sheen'} ),
          |      (cs:Person {name:'Charlie Sheen'}),
          |      p = shortestPath( (ms)-[rels:ACTED_IN*]-(cs) )
          |WHERE ALL(r in rels WHERE exists(r.role))
          |RETURN p""", assertShortestPathLength) {
        p(
          """This query can be evaluated with the fast algorithm -- there are no predicates that need to see the whole
            |path before being evaluated.""")
        profileExecutionPlan()
      }
    }
    section("Shortest path with additional predicate checks on the paths") {
      section("Consider using the exhaustive search as a fallback") {
        p(
          """Predicates used in the `WHERE` clause that apply to the shortest path pattern are evaluated before deciding
            |what the shortest matching path is. """)
        query(
          """MATCH (cs:Person {name:'Charlie Sheen'}),
            |      (ms:Person {name:'Martin Sheen'}),
            |      p = shortestPath( (cs)-[*]-(ms) )
            |WHERE length(p) > 1
            |RETURN p""", assertShortestPathLength) {
          p(
            """This query, in contrast with the one above, needs to check that the whole path follows the predicate
              |before we know if it is valid or not, and so the query plan will also include the fallback to the slower
              |exhaustive search algorithm""")
          profileExecutionPlan()
        }
        p(
          """The way the bigger exhaustive query plan works is by using `Apply`/`Optional` to ensure that when the
            |fast algorithm does not find any results, a `NULL` result is generated instead of simply stopping the result
            |stream.
            |On top of this, the planner will issue an `AntiConditionalApply`, which will run the exhaustive search
            |if the path variable is pointing to `NULL` instead of a path.""")
      }
      section("Prevent the exhaustive search from being used as a fallback") {
        query(
          """MATCH (cs:Person {name:'Charlie Sheen'}),
            |      (ms:Person {name:'Martin Sheen'}),
            |      p = shortestPath( (cs)-[*]-(ms) )
            |WITH p
            |WHERE length(p) > 1
            |RETURN p""", assertShortestPathLength) {
          p(
            """This query, just like the one above, needs to check that the whole path follows the predicate
              |before we know if it is valid or not. However, the inclusion of the `WITH` clause means that the query
              |plan will not include the fallback to the slower exhaustive search algorithm. Instead, any
              |paths found by the fast algorithm will subsequently be filtered, which may result in no answers
              | being returned.""")
          profileExecutionPlan()
        }
      }
    }
  }.build()

  private def assertShortestPathLength = ResultAssertions(result =>
    result.toList.head("p").asInstanceOf[Path].length() shouldBe 2)
}
