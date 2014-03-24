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

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.graphdb.Direction
import org.neo4j.kernel.api.index.IndexDescriptor

class SimpleLogicalPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val context = newMockedLogicalPlanContext()
  val LogicalPlanContext(planContext, estimator, costs, _, _) = context
  val planner = new SimpleLogicalPlanner()
  val pos = DummyPosition(0)

  test("projection only query") {
    // given
    val projections = Map("42" -> SignedIntegerLiteral("42")(pos))
    val qg = QueryGraph(projections, Selections(), Set.empty, Set.empty)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg))

    // then
    resultPlan should equal(Projection(SingleRow(), projections))
  }

  test("simple pattern query") {
    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val qg = QueryGraph(projections, Selections(), Set(IdName("n")), Set.empty)

    when(estimator.estimateAllNodesScan()).thenReturn(1000)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg))

    // then
    resultPlan should equal(AllNodesScan(IdName("n")))
  }

  test("simple label scan without compile-time label id") {
    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val hasLabels = HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")()(pos)))(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> hasLabels)), Set(IdName("n")), Set.empty)

    when(estimator.estimateNodeByLabelScan(None)).thenReturn(1)
    when(estimator.estimateNodeByIdSeek()).thenReturn(2)
    when(estimator.estimateAllNodesScan()).thenReturn(1000)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg))

    // then
    resultPlan should equal(NodeByLabelScan(IdName("n"), Left("Awesome"))())
  }

  test("simple label scan with a compile-time label ID") {
    when(planContext.indexesGetForLabel(12)).thenReturn(Iterator.empty)

    // given
    val projections = Map("n" -> Identifier("n")(pos))
    val labelId = LabelId(12)
    val hasLabels = HasLabels(Identifier("n")(pos), Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> hasLabels)), Set(IdName("n")), Set.empty)

    when(estimator.estimateNodeByLabelScan(Some(labelId))).thenReturn(100)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg))

    // then
    resultPlan should equal(NodeByLabelScan(IdName("n"), Right(labelId))())
  }

  test("simple node by id seek with a node id expression") {
    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    when(estimator.estimateNodeByIdSeek()).thenReturn(1)

    // when
    val resultPlan = planner.plan(context.copy(semanticTable = semanticTable, queryGraph = qg))

    // then
    resultPlan should equal(NodeByIdSeek(IdName("n"), SignedIntegerLiteral("42")(pos), 1)())
  }

  test("simple node by id seek with a collection of node ids") {
    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val expr = In(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
      Collection(
        Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
      )(pos)
    )(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    when(estimator.estimateNodeByIdSeek()).thenReturn(1)

    // when
    val resultPlan = planner.plan(context.copy(semanticTable = semanticTable, queryGraph = qg))

    // then
    resultPlan should equal(NodeByIdSeek(IdName("n"),Collection(
      Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
    )(pos), 3)())
  }

  test("simple relationship by id seek with a rel id expression") {
    // given
    val rIdent = Identifier("r")(pos)
    val fromIdent = Identifier("from")(pos)
    val toIdent = Identifier("to")(pos)
    val projections = Map("r" -> rIdent, "from" -> fromIdent, "to" -> toIdent)
    val expr = Equals(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(rIdent))(pos),
      SignedIntegerLiteral("42")(pos)
    )(pos)
    val from = IdName("from")
    val end = IdName("to")
    val patternRel = PatternRelationship(IdName("r"), (from, end), Direction.OUTGOING, Seq.empty)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("r")) -> expr)), Set(from, end), Set(patternRel))
    val semanticTable = SemanticTableBuilder().withTyping(rIdent -> ExpressionTypeInfo(symbols.CTRelationship)).result()
    when(estimator.estimateRelationshipByIdSeek()).thenReturn(1)

    // when
    val resultPlan = planner.plan(context.copy(semanticTable = semanticTable, queryGraph = qg))

    // then
    resultPlan should equal(DirectedRelationshipByIdSeek(IdName("r"), SignedIntegerLiteral("42")(pos), 1, from, end)())
  }

  test("simple relationship by id seek with a collection of node ids") {
    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val expr = In(
      FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
      Collection(
        Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
      )(pos)
    )(pos)
    val qg = QueryGraph(projections, Selections(Seq(Set(IdName("n")) -> expr)), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    when(estimator.estimateNodeByIdSeek()).thenReturn(1)

    // when
    val resultPlan = planner.plan(context.copy(semanticTable = semanticTable, queryGraph = qg))

    // then
    resultPlan should equal(NodeByIdSeek(IdName("n"),Collection(
      Seq(SignedIntegerLiteral("42")(pos), SignedIntegerLiteral("43")(pos), SignedIntegerLiteral("43")(pos))
    )(pos), 3)())
  }

  // 2014-03-12 - Davide: broken test, we should also check that we get a filter on node id...
  ignore("simple label scan with a compile-time label ID and node ID predicate when label scan is cheaper") {
    when(planContext.indexesGetForLabel(12)).thenReturn(Iterator.empty)

    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val labelId = LabelId(12)
    val predicates = Seq(
      Set(IdName("n")) -> HasLabels(identifier, Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos),
      Set(IdName("n")) ->  Equals(
        FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos)
    )
    val qg = QueryGraph(projections, Selections(predicates), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    when(estimator.estimateNodeByLabelScan(Some(labelId))).thenReturn(1)
    when(estimator.estimateNodeByIdSeek()).thenReturn(100)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg, semanticTable = semanticTable))

    // then
    resultPlan should equal(NodeByLabelScan(IdName("n"), Right(labelId))())
  }

  // 2014-03-12 - Davide: broken test, we should also check that we get a filter on node id...
  ignore("simple label scan with a compile-time label ID and node ID predicate when node by ID is cheaper") {
    when(planContext.indexesGetForLabel(12)).thenReturn(Iterator.empty)

    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val labelId = LabelId(12)
    val predicates = Seq(
      Set(IdName("n")) ->  Equals(
        FunctionInvocation(FunctionName("id")(pos), distinct = false, Array(identifier))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos),
      Set(IdName("n")) -> HasLabels(identifier, Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos)
    )
    when(estimator.estimateNodeByLabelScan(Some(labelId))).thenReturn(100)
    when(estimator.estimateNodeByIdSeek()).thenReturn(1)
    val qg = QueryGraph(projections, Selections(predicates), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    // when
    val resultPlan = planner.plan(context.copy( queryGraph = qg, semanticTable = semanticTable))

    // then
    resultPlan should equal(NodeByIdSeek(IdName("n"), SignedIntegerLiteral("42")(pos), 1)())
  }

  test("index scan when there is an index on the property") {
    when(planContext.indexesGetForLabel(12)).thenAnswer(new Answer[Iterator[IndexDescriptor]] {
      override def answer(invocation: InvocationOnMock) = Iterator(new IndexDescriptor(12, 15))
    })
    when(planContext.uniqueIndexesGetForLabel(12)).thenReturn(Iterator())

    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val labelId = LabelId(12)
    val propertyKeyId = PropertyKeyId(15)
    val predicates = Seq(
      Set(IdName("n")) ->  Equals(
        Property(identifier, PropertyKeyName("prop")(Some(propertyKeyId))(pos))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos),
      Set(IdName("n")) -> HasLabels(identifier, Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos)
    )
    when(estimator.estimateAllNodesScan()).thenReturn(1000)
    when(estimator.estimateNodeByLabelScan(Some(labelId))).thenReturn(100)
    when(estimator.estimateNodeIndexSeek(LabelId(12), propertyKeyId)).thenReturn(1)
    val qg = QueryGraph(projections, Selections(predicates), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg, semanticTable = semanticTable))

    // then
    resultPlan should equal(NodeIndexSeek(IdName("n"), labelId, propertyKeyId, SignedIntegerLiteral("42")(pos))())
  }

  test("index seek when there is an index on the property") {
    when(planContext.indexesGetForLabel(12)).thenReturn(Iterator())
    when(planContext.uniqueIndexesGetForLabel(12)).thenAnswer(new Answer[Iterator[IndexDescriptor]] {
      override def answer(invocation: InvocationOnMock) = Iterator(new IndexDescriptor(12, 15))
    })

    // given
    val identifier = Identifier("n")(pos)
    val projections = Map("n" -> identifier)
    val labelId = LabelId(12)
    val propertyKeyId = PropertyKeyId(15)
    val predicates = Seq(
      Set(IdName("n")) ->  Equals(
        Property(identifier, PropertyKeyName("prop")(Some(propertyKeyId))(pos))(pos),
        SignedIntegerLiteral("42")(pos)
      )(pos),
      Set(IdName("n")) -> HasLabels(identifier, Seq(LabelName("Awesome")(Some(labelId))(pos)))(pos)
    )
    when(estimator.estimateAllNodesScan()).thenReturn(1000)
    when(estimator.estimateNodeByLabelScan(Some(labelId))).thenReturn(100)
    val qg = QueryGraph(projections, Selections(predicates), Set(IdName("n")), Set.empty)
    val semanticTable = SemanticTableBuilder().withTyping(identifier -> ExpressionTypeInfo(symbols.CTNode)).result()

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg, semanticTable = semanticTable))

    // then
    resultPlan should equal(NodeIndexUniqueSeek(IdName("n"), labelId, propertyKeyId, SignedIntegerLiteral("42")(pos))())
  }

  test("simple cartesian product query") {
    // given
    val projections = Map("n" -> Identifier("n")(pos), "m" -> Identifier("m")(pos))
    val qg = QueryGraph(projections, Selections(), Set(IdName("n"), IdName("m")), Set.empty)

    when(estimator.estimateAllNodesScan()).thenReturn(1000)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg))

    // then
    resultPlan should equal(CartesianProduct(AllNodesScan(IdName("n")), AllNodesScan(IdName("m"))))
  }

  test("simple cartesian product query with a predicate on the elements") {
    // given
    val projections = Map("n" -> Identifier("n")(pos), "m" -> Identifier("m")(pos))
    val qg = QueryGraph(projections, Selections(), Set(IdName("n"), IdName("m")), Set.empty)

    when(estimator.estimateAllNodesScan()).thenReturn(1000)

    // when
    val resultPlan = planner.plan(context.copy(queryGraph = qg))

    // then
    resultPlan should equal(CartesianProduct(AllNodesScan(IdName("n")), AllNodesScan(IdName("m"))))
  }
}
