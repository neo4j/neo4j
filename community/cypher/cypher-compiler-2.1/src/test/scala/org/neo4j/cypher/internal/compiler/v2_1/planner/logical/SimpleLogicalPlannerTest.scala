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
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.NodeByIdScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Projection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.{LabelId, DummyPosition}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Projection
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import scala.Some
import scala.Some

class SimpleLogicalPlannerTest extends CypherFunSuite with MockitoSugar {

  val estimator = mock[CardinalityEstimator]
  val planner = new SimpleLogicalPlanner(estimator)
  val pos = DummyPosition(0)

  test("projection only query") {
    // given
    val projections = Map("42" -> SignedIntegerLiteral("42")(pos))
    val qg = QueryGraph(projections, Selections(), Set.empty)

    // when
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(Projection(SingleRow(), projections))
  }

  test("simple pattern query") {
    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val qg = QueryGraph(projections, Selections(), Set(IdName("n")))

    // when
    when(estimator.estimateAllNodes()).thenReturn(1000)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(AllNodesScan(IdName("n"), 1000))
  }

  test("simple label scan without compile-time label ID") {
    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val hasLabels = HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")()(pos)))(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> hasLabels)), Set(IdName("n")))

    // when
    when(estimator.estimateNodeByLabelScan(None)).thenReturn(0)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(NodeByLabelScan(IdName("n"), Left("Awesome"), 0))
  }

  test("simple label scan with a compile-time label ID") {
    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val labelId = LabelId(12)
    val hasLabels = HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> hasLabels)), Set(IdName("n")))

    // when
    when(estimator.estimateNodeByLabelScan(Some(labelId))).thenReturn(100)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(NodeByLabelScan(IdName("n"), Right(labelId), 100))
  }

  test("simple node by id scan with a node id expression") {
    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val expr = Equals(
      FunctionInvocation(Identifier("id")(pos), distinct = false, Array(Identifier("n")(pos)))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")))

    // when
    when(estimator.estimateNodeByIdScan()).thenReturn(1)
    val resultPlan = planner.plan(qg)

    // then
    resultPlan should equal(NodeByIdScan(IdName("n"), SignedIntegerLiteral("42")(pos), 1))
  }
}
