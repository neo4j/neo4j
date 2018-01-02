/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ast._
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, IdName, PlannerQuery}
import org.neo4j.cypher.internal.planner.v3_4.spi.TokenContext
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, NonEmptyList}
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans.{AllNodesScan, ProduceResult, Selection, _}

class SlottedRewriterTest extends CypherFunSuite with AstConstructionTestSupport {
  private val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))
  private def propFor(v: String, key: String) = Property(Variable(v)(pos), PropertyKeyName(key)(pos))(pos)
  private val xProp = propFor("x", "prop")
  private val aProp = propFor("a", "prop")
  private val bProp = propFor("b", "prop")
  private val nProp = propFor("n", "prop")
  private val rProp = propFor("r", "prop")

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n") {
    val allNodes = AllNodesScan(IdName("x"), Set.empty)(solved)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)(solved)
    val produceResult = ProduceResult(selection, Seq("x"))
    produceResult.assignIds()
    val offset = 0
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = false, CTNode)
    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      allNodes.assignedId -> slots,
      selection.assignedId -> slots,
      produceResult.assignedId -> slots)
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)
    val result = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodeProperty(offset, tokenId, "x.prop")(xProp), literalInt(42))(pos)

    result should equal(ProduceResult(Selection(Seq(newPredicate), allNodes)(solved), Seq("x")))
    lookup(result.assignedId) should equal(slots)
  }

  test("comparing two relationship ids simpler") {
    // match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = IdName("a")
    val node2 = IdName("b")
    val node3 = IdName("c")
    val rel1 = IdName("r1")
    val rel2 = IdName("r2")
    val argument = Argument(Set(node1, node2, node3, rel1, rel2))(solved)
    val predicate = Not(Equals(varFor("r1"), varFor("r2"))(pos))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    selection.assignIds()
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r1", nullable = false, CTRelationship).
      newLong("c", nullable = false, CTNode).
      newLong("r2", nullable = false, CTRelationship)

    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      argument.assignedId -> slots,
      selection.assignedId -> slots
    )
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then
    result should equal(Selection(Seq(Not(PrimitiveEquals(IdFromSlot(2), IdFromSlot(4)))(pos)), argument)(solved))
    lookup(result.assignedId) should equal(slots)
  }

  test("comparing two relationship ids simpler when they are null") {
    // optional match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = IdName("a")
    val node2 = IdName("b")
    val node3 = IdName("c")
    val rel1 = IdName("r1")
    val rel2 = IdName("r2")
    val argument = Argument(Set(node1, node2, node3, rel1, rel2))(solved)
    val predicate = Not(Equals(varFor("r1"), varFor("r2"))(pos))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    selection.assignIds()
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r1", nullable = true, CTNode).
      newLong("c", nullable = false, CTNode).
      newLong("r2", nullable = true, CTNode)

    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      argument.assignedId -> slots,
      selection.assignedId -> slots
    )
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then
    val rewrittenPredicate =
      NullCheck(2,
        NullCheck(4,
          Not(
            PrimitiveEquals(
              IdFromSlot(2),
              IdFromSlot(4)))(pos)))
    result should equal(Selection(Seq(rewrittenPredicate), argument)(solved))
    lookup(result.assignedId) should equal(slots)
  }

  test("comparing different types must check nulls before returning shortcut") {
    // optional match (a)-[r1]->() where not(r1 = a)
    // given
    val node1 = IdName("a")
    val node2 = IdName("b")
    val rel = IdName("r")
    val argument = Argument(Set(node1, node2, rel))(solved)
    val predicate = Equals(varFor("r"), varFor("a"))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    selection.assignIds()
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = true, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r", nullable = true, CTRelationship)

    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      argument.assignedId -> slots,
      selection.assignedId -> slots
    )
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then since we are doing a not(t1 = t2), we shortcut to True if neither value is null
    val rewrittenPredicate =
      NullCheck(2,
        NullCheck(0,
          False()(pos)))

    result should equal(Selection(Seq(rewrittenPredicate), argument)(solved))
    lookup(result.assignedId) should equal(slots)
  }

  test("return nullable node") {
    // match optional (a) return (a)
    // given
    val node1 = IdName("a")
    val argument = AllNodesScan(node1, Set.empty)(solved)
    val predicate = Equals(prop("a", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    selection.assignIds()
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = true, CTNode)

    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      argument.assignedId -> slots,
      selection.assignedId -> slots
    )
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then
    val expectedPredicate = Equals(NullCheckProperty(0, NodeProperty(0, 666, "a.prop")(aProp)), literalInt(42))(pos)
    result should equal(Selection(Seq(expectedPredicate), argument)(solved))
    lookup(result.assignedId) should equal(slots)
  }

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n when token is unknown") {
    val allNodes = AllNodesScan(IdName("x"), Set.empty)(solved)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)(solved)
    val produceResult = ProduceResult(selection, Seq("x"))
    produceResult.assignIds()
    val offset = 0
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = false, CTNode)
    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      allNodes.assignedId -> slots,
      selection.assignedId -> slots,
      produceResult.assignedId -> slots)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    val result = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodePropertyLate(offset, "prop", "x.prop")(xProp), literalInt(42))(pos)

    result should equal(ProduceResult(Selection(Seq(newPredicate), allNodes)(solved), Seq("x")))
    lookup(result.assignedId) should equal(slots)
  }

  test("reading property key when the token does not exist at compile time") {
    // match (a)-[r1]->(b) where r.prop = 42
    // given
    val node1 = IdName("a")
    val node2 = IdName("b")
    val edge = IdName("r")
    val argument = Argument(Set(node1, node2, edge))(solved)
    val predicate = Equals(prop("r", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    selection.assignIds()
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r", nullable = false, CTRelationship)

    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      argument.assignedId -> slots,
      selection.assignedId -> slots
    )
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    result should equal(Selection(Seq(Equals(RelationshipPropertyLate(2, "prop", "r.prop")(rProp), literalInt(42))(pos)), argument)(solved))
    lookup(result.assignedId) should equal(slots)
  }

  test("projection with map lookup MATCH (n) RETURN n.prop") {
    // given
    val node = Variable("n")(pos)
    val allNodes = AllNodesScan(IdName.fromVariable(node), Set.empty)(solved)
    val projection = Projection(allNodes, Map("n.prop" -> prop("n", "prop")))(solved)
    val produceResult = ProduceResult(projection, Seq("n.prop"))
    produceResult.assignIds()
    val nodeOffset = 0
    val slots = SlotConfiguration.empty.
      newLong("n", nullable = false, CTNode).
      newReference("n.prop", nullable = true, CTAny)
    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      allNodes.assignedId -> slots,
      projection.assignedId -> slots,
      produceResult.assignedId -> slots)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)

    //when
    val result = rewriter(produceResult, lookup)

    //then
    val newProjection = Projection(allNodes, Map("n.prop" -> NodePropertyLate(nodeOffset, "prop", "n.prop")(nProp)))(solved)
    result should equal(
      ProduceResult(newProjection, Seq("n.prop")))
    lookup(result.assignedId) should equal(slots)
  }

  test("rewriting variable should always work, even if Variable is not part of a bigger tree") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)
    projection.assignIds()
    val tokenContext = mock[TokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = false, CTNode).
      newReference("x.propertyKey", nullable = true, CTAny)

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val resultPlan = rewriter(projection, Map(leaf.assignedId -> slots, projection.assignedId -> slots))

    // then
    resultPlan should equal(
      Projection(leaf, Map(
        "x" -> NodeFromSlot(0, "x"),
        "x.propertyKey" -> NodeProperty(slots.getLongOffsetFor("x"), tokenId, "x.propertyKey")(propFor("x", "propertyKey"))
      ))(solved)
    )
  }

  test("make sure to handle nullable nodes correctly") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)
    projection.assignIds()
    val tokenContext = mock[TokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = true, CTNode).
      newReference("x.propertyKey", nullable = true, CTAny)

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val resultPlan = rewriter(projection, Map(leaf.assignedId -> slots, projection.assignedId -> slots))

    // then
    val nodeOffset = slots.getLongOffsetFor("x")
    resultPlan should equal(
      Projection(leaf, Map(
        "x" -> NullCheckVariable(0, NodeFromSlot(0, "x")),
        "x.propertyKey" -> NullCheckProperty(nodeOffset, NodeProperty(nodeOffset, tokenId, "x.propertyKey")(propFor("x", "propertyKey")))
      ))(solved)
    )
  }

  test("argument on two sides of Apply") {
    val sr1 = Argument()(solved)
    val sr2 = Argument()(solved)
    val pr1A = Projection(sr1, Map("x" -> literalInt(42)))(solved)
    val pr1B = Projection(pr1A, Map("xx" -> varFor("x")))(solved)
    val pr2 = Projection(sr2, Map("y" -> literalInt(666)))(solved)
    val apply = Apply(pr1B, pr2)(solved)
    apply.assignIds()

    val lhsPipeline = SlotConfiguration.empty.
      newReference("x", nullable = true, CTAny).
      newReference("xx", nullable = true, CTAny)

    val rhsPipeline = SlotConfiguration.empty.
      newReference("y", nullable = true, CTAny)

    // when
    val rewriter = new SlottedRewriter(mock[TokenContext])
    val lookup = Map(sr1.assignedId -> lhsPipeline, pr1A.assignedId -> lhsPipeline, pr1B.assignedId -> lhsPipeline,
      sr2.assignedId -> rhsPipeline, pr2.assignedId -> rhsPipeline, apply.assignedId -> rhsPipeline)
    val resultPlan = rewriter(apply, lookup)

    // then

    val pr1BafterRewrite = Projection(pr1A, Map("xx" -> ReferenceFromSlot(0, "x")))(solved)
    val applyAfterRewrite = Apply(pr1BafterRewrite, pr2)(solved)

    resultPlan should equal(
      applyAfterRewrite
    )

    lookup(resultPlan.assignedId) should equal(rhsPipeline)
    lookup(sr1.assignedId) should equal(lhsPipeline)
    lookup(sr2.assignedId) should equal(rhsPipeline)
  }

  test("ValueHashJoin needs to execute expressions with two different slots") {
    // MATCH (a:labelA), (b:labelB) WHERE a.prop = b.prop
    val leafA = NodeByLabelScan(IdName("a"), LabelName("labelA")(pos), Set.empty)(solved)
    val leafB = NodeByLabelScan(IdName("b"), LabelName("labelB")(pos), Set.empty)(solved)

    val lhsExp = prop("a", "prop")
    val rhsExp = prop("b", "prop")
    val join = ValueHashJoin(leafA, leafB, Equals(lhsExp, rhsExp)(pos))(solved)

    join.assignIds()

    val lhsPipeline = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode)

    val rhsPipeline = SlotConfiguration.empty.
      newLong("b", nullable = false, CTNode)

    val joinPipeline = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode)

    val tokenId = 666
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val lookup = Map(leafA.assignedId -> lhsPipeline, leafB.assignedId -> rhsPipeline, join.assignedId -> joinPipeline)
    val resultPlan = rewriter(join, lookup)

    // then
    val lhsExpAfterRewrite = NodeProperty(0, tokenId, "a.prop")(aProp)
    val rhsExpAfterRewrite = NodeProperty(0, tokenId, "b.prop")(bProp)  // Same offsets, but on different contexts
    val joinAfterRewrite = ValueHashJoin(leafA, leafB, Equals(lhsExpAfterRewrite, rhsExpAfterRewrite)(pos))(solved)

    resultPlan should equal(
      joinAfterRewrite
    )

    lookup(resultPlan.assignedId) should equal(joinPipeline)
    lookup(leafA.assignedId) should equal(lhsPipeline)
    lookup(leafB.assignedId) should equal(rhsPipeline)
  }

  test("selection with null checks against a primitive LongSlot") {
    // given
    val allNodes = AllNodesScan(IdName("x"), Set.empty)(solved)
    val predicate = IsNull(varFor("x"))(pos)
    val selection = Selection(Seq(predicate), allNodes)(solved)
    val produceResult = ProduceResult(selection, Seq("x"))
    produceResult.assignIds()

    val offset = 0
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = true, CTNode)
    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      allNodes.assignedId -> slots,
      selection.assignedId -> slots,
      produceResult.assignedId -> slots)
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup)

    // then
    val newPredicate = IsPrimitiveNull(offset)
    result should equal(ProduceResult(Selection(Seq(newPredicate), allNodes)(solved), Seq("x")))
    lookup(result.assignedId) should equal(slots)
  }

  test("selection between two references") {
    // given
    val arg = Argument(Set(IdName("x"), IdName("z")))(solved)
    val predicate1 = Equals(varFor("x"), varFor("z"))(pos)
    val predicate2 = Not(Equals(varFor("x"), varFor("z"))(pos))(pos)
    val selection = Selection(Seq(predicate1, predicate2), arg)(solved)
    val produceResult = ProduceResult(selection, Seq("x", "z"))
    produceResult.assignIds()

    val offsetX = 0
    val offsetZ = 1
    val slots = SlotConfiguration.empty.
      newReference("x", nullable = true, CTAny).
      newReference("z", nullable = true, CTAny)
    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      arg.assignedId -> slots,
      selection.assignedId -> slots,
      produceResult.assignedId -> slots)
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup)

    // then
    val newPred1 = Equals(ReferenceFromSlot(offsetX, "x"), ReferenceFromSlot(offsetZ, "z"))(pos)
    val newPred2 = Not(Equals(ReferenceFromSlot(offsetX, "x"), ReferenceFromSlot(offsetZ, "z"))(pos))(pos)
    result should equal(ProduceResult(Selection(Seq(newPred1, newPred2), arg)(solved), Seq("x", "z")))
    lookup(result.assignedId) should equal(slots)
  }

  test("should be able to rewrite expressions declared as Variable or Property") {
    // given
    val arg = Argument()(solved)
    val predicate = AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(LessThan(literalInt(42), varFor("z"))(pos)))
    val selection = Selection(Seq(predicate), arg)(solved)
    selection.assignIds()

    val offsetN = 0
    val offsetZ = 0
    val slots = SlotConfiguration.empty.
      newLong("n", nullable = true, CTNode).
      newReference("z", nullable = false, CTAny)
    val lookup: Map[LogicalPlanId, SlotConfiguration] = Map(
      arg.assignedId -> SlotConfiguration.empty,
      selection.assignedId -> slots)
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then
    val newPred = AndedPropertyInequalities(NullCheckVariable(0, NodeFromSlot(offsetN, "n")),
      NullCheckProperty(offsetN, NodeProperty(offsetN, 666, "n.prop")(xProp)),
      NonEmptyList(LessThan(literalInt(42), ReferenceFromSlot(offsetZ, "z"))(pos)))
    result should equal(Selection(Seq(newPred), arg)(solved))
  }
}
