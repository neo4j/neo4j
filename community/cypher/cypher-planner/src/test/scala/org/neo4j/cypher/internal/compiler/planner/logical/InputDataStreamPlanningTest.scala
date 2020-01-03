/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.phases._
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.v4_0.frontend.helpers.{InputDataStreamTestInitialState, InputDataStreamTestParsing}
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

import scala.util.Random

class InputDataStreamPlanningTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("INPUT DATA STREAM a, b, c RETURN *") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c RETURN *")._2 should equal(Input(Seq("a", "b", "c")))
  }

  test("INPUT DATA STREAM a, b, c RETURN DISTINCT a") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c RETURN DISTINCT a")._2 should equal(Distinct(Input(Seq("a", "b", "c")), Map("a" -> varFor("a"))))
  }

  test("INPUT DATA STREAM a, b, c RETURN sum(a)") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c RETURN sum(a)")._2 should equal(
      Aggregation(Input(Seq("a", "b", "c")), Map.empty, Map("sum(a)" -> sum(varFor("a"))))
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * WHERE a.pid = 99 RETURN *") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c WITH * WHERE a.pid = 99 RETURN *")._2 should equal(
      Selection(ands(propEquality("a", "pid", 99)), Input(Seq("a", "b", "c")))
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * WHERE a:Employee RETURN a.name AS name ORDER BY name") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c WITH * WHERE a:Employee RETURN a.name AS name ORDER BY name")._2 should equal(
      Sort(
        Projection(
          Selection(ands(hasLabels("a", "Employee")), Input(Seq("a", "b", "c"))), Map("name" -> prop("a", "name"))
        ),
        List(Ascending("name"))
      )
    )
  }

  test("INPUT DATA STREAM a, b, c WITH * MATCH (x) RETURN *") {
    new given().getLogicalPlanFor("INPUT DATA STREAM a, b, c WITH * MATCH (x) RETURN *")._2 should equal(
      Apply(
        Input(Seq("a", "b", "c")),
        AllNodesScan("x", Set("a", "b", "c")
        )
      )
    )
  }

  test("INPUT DATA STREAM g, uid, cids, cid, p RETURN *") {
    new given().getLogicalPlanFor("INPUT DATA STREAM g, uid, cids, cid, p RETURN *")._2 should equal(
      Input(Seq("g", "uid", "cids", "cid", "p")))
  }

  test("INPUT DATA STREAM with large number of columns") {
    val randomColumns = Random.shuffle(for (c <- 'a' to 'z'; n <- 1 to 10) yield s"$c$n")
    new given().getLogicalPlanFor(s"INPUT DATA STREAM ${randomColumns.mkString(",")} RETURN *")._2 should equal(
      Input(randomColumns))
  }

  override def pipeLine(): Transformer[PlannerContext, BaseState, LogicalPlanState] = {
    InputDataStreamTestParsing andThen super.pipeLine()
  }

  // Both the test parser that understands INPUT DATA STREAM and the standard parser are part of the pipeline,
  // the standard parser will be fed with 'RETURN 1' just to make it happy
  override def createInitState(queryString: String): BaseState = InputDataStreamTestInitialState(queryString, "RETURN 1", None, IDPPlannerName)

}
