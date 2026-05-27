/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.plandescription.Arguments.BatchSize
import org.neo4j.cypher.internal.plandescription.Arguments.DbHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheHits
import org.neo4j.cypher.internal.plandescription.Arguments.PageCacheMisses
import org.neo4j.cypher.internal.plandescription.Arguments.Planner
import org.neo4j.cypher.internal.plandescription.Arguments.Rows
import org.neo4j.cypher.internal.plandescription.Arguments.Runtime
import org.neo4j.cypher.internal.plandescription.Arguments.RuntimeVersion
import org.neo4j.cypher.internal.plandescription.Arguments.Time
import org.neo4j.cypher.internal.plandescription.Arguments.Version
import org.neo4j.cypher.internal.util.attribution.Id

class InternalPlanDescriptionTest extends CommunityCypherTestSuite {

  private val ID = Id(0)

  test("arguments are read as expected") {
    val arguments = Seq(
      DbHits(1),
      PageCacheHits(2),
      PageCacheMisses(3),
      Time(4),
      Rows(5)
    )
    val plan = PlanDescriptionImpl(ID, "plan", Seq.empty, arguments, Set())
    plan.hasProfilerStatistics should equal(true)
    plan.getProfilerStatistics.getDbHits should equal(1)
    plan.getProfilerStatistics.getPageCacheHits should equal(2)
    plan.getProfilerStatistics.getPageCacheMisses should equal(3)
    plan.getProfilerStatistics.getTime should equal(4)
    plan.getProfilerStatistics.getRows should equal(5)
  }

  test("flatten behaves like expected for plan with two children") {
    val child1 = PlanDescriptionImpl(ID, "child1", Seq.empty, Seq.empty, Set())
    val child2 = PlanDescriptionImpl(ID, "child2", Seq.empty, Seq.empty, Set())
    val top = PlanDescriptionImpl(ID, "top", Seq(child1, child2), Seq.empty, Set())

    top.flatten should equal(Seq(top, child1, child2))
  }

  test("single plan flattened stays single") {
    val single = PlanDescriptionImpl(ID, "single", Seq.empty, Seq.empty, Set())

    single.flatten should equal(Seq(single))
  }

  test("left leaning plan should also flatten out nicely") {
    val leaf = PlanDescriptionImpl(ID, "leaf", Seq.empty, Seq.empty, Set())
    val lvl1 = PlanDescriptionImpl(ID, "lvl1", Seq(leaf), Seq.empty, Set())
    val lvl2 = PlanDescriptionImpl(ID, "lvl2", Seq(lvl1), Seq.empty, Set())
    val root = PlanDescriptionImpl(ID, "root", Seq(lvl2), Seq.empty, Set())

    root.flatten should equal(Seq(root, lvl2, lvl1, leaf))
  }

  test("bushy tree flattens correctly") {
    /*
                  A
             B1      B2
           C1  C2  C3  C4
     */
    val C4 = PlanDescriptionImpl(ID, "C4", Seq.empty, Seq.empty, Set())
    val C3 = PlanDescriptionImpl(ID, "C3", Seq.empty, Seq.empty, Set())
    val C2 = PlanDescriptionImpl(ID, "C2", Seq.empty, Seq.empty, Set())
    val C1 = PlanDescriptionImpl(ID, "C1", Seq.empty, Seq.empty, Set())
    val B2 = PlanDescriptionImpl(ID, "B2", Seq(C3, C4), Seq.empty, Set())
    val B1 = PlanDescriptionImpl(ID, "B1", Seq(C1, C2), Seq.empty, Set())
    val A = PlanDescriptionImpl(ID, "A", Seq(B1, B2), Seq.empty, Set())

    A.flatten should equal(Seq(A, B1, C1, C2, B2, C3, C4))
  }

  test("toString should render nicely") {
    val version = "5.0"
    val planDescription = PlanDescriptionImpl(ID, "Leaf", Seq.empty, Seq.empty, Set())
      .addArgument(Version(version))
      .addArgument(Planner("COST"))
      .addArgument(RuntimeVersion(version))
      .addArgument(Runtime("PIPELINED"))
      .addArgument(BatchSize(128))

    normalizeNewLines(planDescription.toString) should equal(
      normalizeNewLines(s"""Cypher $version
                           |
                           |Planner COST
                           |
                           |Runtime PIPELINED
                           |
                           |Runtime version $version
                           |
                           |Batch size 128
                           |
                           |+----------+----+
                           || Operator | Id |
                           |+----------+----+
                           || +Leaf    |  0 |
                           |+----------+----+
                           |
                           |Total database accesses: ?
                           |""".stripMargin)
    )
  }
}
