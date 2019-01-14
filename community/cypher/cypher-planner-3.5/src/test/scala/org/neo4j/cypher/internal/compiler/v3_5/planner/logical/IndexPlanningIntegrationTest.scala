/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class IndexPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should not plan index usage if predicate depends on variable from same QueryGraph") {

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan =
        new given {
          indexOn("Label", "prop")
        } getLogicalPlanFor s"MATCH (a)-->(b:Label) WHERE b.prop $op a.prop RETURN a"

      plan._2 should beLike {
        case Selection(_,
              Expand(
                _:NodeIndexScan | _:NodeByLabelScan,
                _, _, _, _, _, _
              )
             ) => ()
      }
    }
  }

  test("should plan index usage if predicate depends on simple variable from horizon") {

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan =
        new given {
          indexOn("Label", "prop")
        } getLogicalPlanFor s"WITH 'foo' AS foo MATCH (a)-->(b:Label) WHERE b.prop $op foo RETURN a"

      plan._2 should beLike {
        case Expand(
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek | _:NodeIndexContainsScan | _:NodeIndexEndsWithScan
              ), _, _, _, _, _, _) => ()
      }
    }
  }

  test("should plan index usage if predicate depends on property of variable from horizon") {

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan =
        new given {
          indexOn("Label", "prop")
        } getLogicalPlanFor s"WITH {prop: 'foo'} AS foo MATCH (a)-->(b:Label) WHERE b.prop $op foo.prop RETURN a"

      plan._2 should beLike {
        case Expand(
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek | _:NodeIndexContainsScan | _:NodeIndexEndsWithScan
              ), _, _, _, _, _, _) => ()
      }
    }
  }

  test("should not plan index usage if distance predicate depends on variable from same QueryGraph") {

    val plan =
      new given {
        indexOn("Place", "location")
      } getLogicalPlanFor
        s"""MATCH (p:Place)-->(x:Preference)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
           |RETURN p.location as point
        """.stripMargin

    plan._2 should beLike {
      case Projection(
            Selection(_,
              Expand(
                _:NodeByLabelScan, _, _, _, _, _, _
              )
            ), _) => ()
    }
  }

  test("should plan index usage if distance predicate depends on variable from the horizon") {

    val plan =
      new given {
        indexOn("Place", "location")
      } getLogicalPlanFor
        s"""WITH 10 AS maxDistance
           |MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
           |RETURN p.location as point
        """.stripMargin

    plan._2 should beLike {
      case Projection(
            Selection(_,
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek
              )
            ), _) => ()
    }
  }

  test("should plan index usage if distance predicate depends on property read of variable from the horizon") {

    val plan =
      new given {
        indexOn("Place", "location")
      } getLogicalPlanFor
        s"""WITH {maxDistance: 10} AS x
           |MATCH (p:Place)
           |WHERE distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
           |RETURN p.location as point
        """.stripMargin

    plan._2 should beLike {
      case Projection(
            Selection(_,
              Apply(
                Projection(_: Argument, _),
                _:NodeIndexSeek
              )
            ), _) => ()
    }
  }
}
