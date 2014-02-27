/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Id
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Projection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast.{LabelName, HasLabels, Identifier, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.{LabelId, DummyPosition}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._

class SimpleLogicalPlannerTest extends CypherFunSuite with MockitoSugar {

  val estimator = mock[CardinalityEstimator]
  val planner = new SimpleLogicalPlanner(estimator)
  val pos = DummyPosition(0)

  test("projection only query") {
    // given
    val expressions = Map("42" -> SignedIntegerLiteral("42")(pos))
    val qg = QueryGraph(expressions, Selections(), Set.empty)

    // when
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(Projection(SingleRow(), expressions))
  }

  test("simple pattern query") {
    // given
    val expressions = Map("n" -> Identifier("n")(pos))
    val qg = QueryGraph(expressions, Selections(), Set(Id("n")))

    // when
    when(estimator.estimateAllNodes()).thenReturn(1000)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(AllNodesScan(Id("n"), 1000))
  }

  test("simple label scan without compile-time label ID") {
    // given
    val expressions = Map("n" -> Identifier("n")(pos))
    val hasLabels = HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")()(pos)))(pos)
    val qg = QueryGraph(expressions, Selections(Seq(Set(Id("n")) -> hasLabels)), Set(Id("n")))

    // when
    when(estimator.estimateLabelScan(None)).thenReturn(0)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(LabelNodesScan(Id("n"), Left("Awesome"), 0))
  }

  test("simple label scan with a compile-time label ID") {
    // given
    val expressions = Map("n" -> Identifier("n")(pos))
    val labelId = LabelId(12)
    val hasLabels = HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos)
    val qg = QueryGraph(expressions, Selections(Seq(Set(Id("n")) -> hasLabels)), Set(Id("n")))

    // when
    when(estimator.estimateLabelScan(Some(labelId))).thenReturn(100)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(LabelNodesScan(Id("n"), Right(labelId), 100))
  }
}
