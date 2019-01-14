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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.logical.plans._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class OrLeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should not explode for many STARTS WITH") {
    val query =
      """MATCH (tn:X) USING INDEX tn:X(prop)
        |WHERE (tn:Y) AND ((
        |   tn.prop STARTS WITH {p3} OR
        |   tn.prop STARTS WITH {p4} OR
        |   tn.prop STARTS WITH {p5} OR
        |   tn.prop STARTS WITH {p6} OR
        |   tn.prop STARTS WITH {p7} OR
        |   tn.prop STARTS WITH {p8} OR
        |   tn.prop STARTS WITH {p9} OR
        |   tn.prop STARTS WITH {p10} OR
        |   tn.prop STARTS WITH {p11} OR
        |   tn.prop STARTS WITH {p12} OR
        |   tn.prop STARTS WITH {p13} OR
        |   tn.prop STARTS WITH {p14} OR
        |   tn.prop STARTS WITH {p15} OR
        |   tn.prop STARTS WITH {p16} OR
        |   tn.prop STARTS WITH {p17} OR
        |   tn.prop STARTS WITH {p18} OR
        |   tn.prop STARTS WITH {p19} OR
        |   tn.prop STARTS WITH {p20} OR
        |   tn.prop STARTS WITH {p21} OR
        |   tn.prop STARTS WITH {p22} OR
        |   tn.prop STARTS WITH {p23} OR
        |   tn.prop STARTS WITH {p24} OR
        |   tn.prop STARTS WITH {p25} OR
        |   tn.prop STARTS WITH {p26} OR
        |   tn.prop STARTS WITH {p27} OR
        |   tn.prop STARTS WITH {p28} OR
        |   tn.prop STARTS WITH {p29} OR
        |   tn.prop STARTS WITH {p30} OR
        |   tn.prop STARTS WITH {p31} OR
        |   tn.prop STARTS WITH {p32} OR
        |   tn.prop STARTS WITH {p33} OR
        |   tn.prop STARTS WITH {p34} OR
        |   tn.prop STARTS WITH {p35} OR
        |   tn.prop STARTS WITH {p36} OR
        |   tn.prop STARTS WITH {p37} OR
        |   tn.prop STARTS WITH {p38} OR
        |   tn.prop STARTS WITH {p39} OR
        |   tn.prop STARTS WITH {p40} OR
        |   tn.prop STARTS WITH {p41} OR
        |   tn.prop STARTS WITH {p42} OR
        |   tn.prop STARTS WITH {p43} OR
        |   tn.prop STARTS WITH {p44} OR
        |   tn.prop STARTS WITH {p45} OR
        |   tn.prop STARTS WITH {p46} OR
        |   tn.prop STARTS WITH {p47} OR
        |   tn.prop STARTS WITH {p48} OR
        |   tn.prop STARTS WITH {p49} OR
        |   tn.prop STARTS WITH {p50} OR
        |   tn.prop STARTS WITH {p51} OR
        |   tn.prop STARTS WITH {p52} OR
        |   tn.prop STARTS WITH {p53} OR
        |   tn.prop STARTS WITH {p54} OR
        |   tn.prop STARTS WITH {p55} OR
        |   tn.prop STARTS WITH {p56} OR
        |   tn.prop STARTS WITH {p57} OR
        |   tn.prop STARTS WITH {p58} OR
        |   tn.prop STARTS WITH {p59} OR
        |   tn.prop STARTS WITH {p60} OR
        |   tn.prop STARTS WITH {p61} OR
        |   tn.prop STARTS WITH {p62} OR
        |   tn.prop STARTS WITH {p63} OR
        |   tn.prop STARTS WITH {p64} OR
        |   tn.prop STARTS WITH {p65} OR
        |   tn.prop STARTS WITH {p66} OR
        |   tn.prop STARTS WITH {p67} OR
        |   tn.prop STARTS WITH {p68} OR
        |   tn.prop STARTS WITH {p69} OR
        |   tn.prop STARTS WITH {p70} OR
        |   tn.prop STARTS WITH {p71} OR
        |   tn.prop STARTS WITH {p72} OR
        |   tn.prop STARTS WITH {p73} OR
        |   tn.prop STARTS WITH {p74} OR
        |   tn.prop STARTS WITH {p75} OR
        |   tn.prop STARTS WITH {p76} OR
        |   tn.prop STARTS WITH {p77} OR
        |   tn.prop STARTS WITH {p78} OR
        |   tn.prop STARTS WITH {p79} OR
        |   tn.prop STARTS WITH {p80} OR
        |   tn.prop STARTS WITH {p81} OR
        |   tn.prop STARTS WITH {p82} OR
        |   tn.prop STARTS WITH {p83} OR
        |   tn.prop STARTS WITH {p84} OR
        |   tn.prop STARTS WITH {p85} OR
        |   tn.prop STARTS WITH {p86} OR
        |   tn.prop STARTS WITH {p87} OR
        |   tn.prop STARTS WITH {p88} OR
        |   tn.prop STARTS WITH {p89} OR
        |   tn.prop STARTS WITH {p90} OR
        |   tn.prop STARTS WITH {p91} OR
        |   tn.prop STARTS WITH {p92}
        | ) AND
        |   tn.processType IN {p0}
        | AND
        |   tn.status IN {p1}
        | AND
        |   tn.fileCollectionEnabled = $p2
        |)
        |RETURN tn""".stripMargin

    val plan = runWithTimeout(1000)((new given {
      indexOn("X", "prop")
    } getLogicalPlanFor query)._2)

    plan.treeCount {
      case _: NodeIndexSeek => true
    } should be(90)
  }

  private def runWithTimeout[T](timeout: Long)(f: => T) : T = {
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(scala.concurrent.Future(f), Duration.apply(timeout, "s"))
  }
}
