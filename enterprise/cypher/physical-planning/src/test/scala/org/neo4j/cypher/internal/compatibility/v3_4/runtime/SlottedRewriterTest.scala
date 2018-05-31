/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.ast._
import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.v3_5.logical.plans.{AllNodesScan, ProduceResult, Selection, _}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.NonEmptyList
import org.opencypher.v9_0.util.attribution.SequentialIdGen
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class SlottedRewriterTest extends CypherFunSuite with AstConstructionTestSupport {
  implicit val idGen = new SequentialIdGen()
  private def propFor(v: String, key: String) = Property(Variable(v)(pos), PropertyKeyName(key)(pos))(pos)
  private val xProp = propFor("x", "prop")
  private val aProp = propFor("a", "prop")
  private val bProp = propFor("b", "prop")
  private val nProp = propFor("n", "prop")
  private val rProp = propFor("r", "prop")

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n") {
    val allNodes = AllNodesScan("x", Set.empty)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)
    val produceResult = ProduceResult(selection, Seq("x"))
    val offset = 0
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = false, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)
    val result = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodeProperty(offset, tokenId, "x.prop")(xProp), literalInt(42))(pos)

    result should equal(ProduceResult(Selection(Seq(newPredicate), allNodes), Seq("x")))
    lookup(result.id) should equal(slots)
  }

  test("comparing two relationship ids simpler") {
    // match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = "a"
    val node2 = "b"
    val node3 = "c"
    val rel1 = "r1"
    val rel2 = "r2"
    val argument = Argument(Set(node1, node2, node3, rel1, rel2))
    val predicate = Not(Equals(varFor("r1"), varFor("r2"))(pos))(pos)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r1", nullable = false, CTRelationship).
      newLong("c", nullable = false, CTNode).
      newLong("r2", nullable = false, CTRelationship)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then
    result should equal(Selection(Seq(Not(PrimitiveEquals(IdFromSlot(2), IdFromSlot(4)))(pos)), argument))
    lookup(result.id) should equal(slots)
  }

  test("comparing two relationship ids simpler when they are null") {
    // optional match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = "a"
    val node2 = "b"
    val node3 = "c"
    val rel1 = "r1"
    val rel2 = "r2"
    val argument = Argument(Set(node1, node2, node3, rel1, rel2))
    val predicate = Not(Equals(varFor("r1"), varFor("r2"))(pos))(pos)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r1", nullable = true, CTNode).
      newLong("c", nullable = false, CTNode).
      newLong("r2", nullable = true, CTNode)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
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
    result should equal(Selection(Seq(rewrittenPredicate), argument))
    lookup(result.id) should equal(slots)
  }

  test("comparing different types must check nulls before returning shortcut") {
    // optional match (a)-[r1]->() where not(r1 = a)
    // given
    val node1 = "a"
    val node2 = "b"
    val rel = "r"
    val argument = Argument(Set(node1, node2, rel))
    val predicate = Equals(varFor("r"), varFor("a"))(pos)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = true, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r", nullable = true, CTRelationship)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then since we are doing a not(t1 = t2), we shortcut to True if neither value is null
    val rewrittenPredicate =
      NullCheck(2,
        NullCheck(0,
          False()(pos)))

    result should equal(Selection(Seq(rewrittenPredicate), argument))
    lookup(result.id) should equal(slots)
  }

  test("return nullable node") {
    // match optional (a) return (a)
    // given
    val node1 = "a"
    val argument = AllNodesScan(node1, Set.empty)
    val predicate = Equals(prop("a", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = true, CTNode)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    // then
    val expectedPredicate = Equals(NullCheckProperty(0, NodeProperty(0, 666, "a.prop")(aProp)), literalInt(42))(pos)
    result should equal(Selection(Seq(expectedPredicate), argument))
    lookup(result.id) should equal(slots)
  }

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n when token is unknown") {
    val allNodes = AllNodesScan("x", Set.empty)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)
    val produceResult = ProduceResult(selection, Seq("x"))
    val offset = 0
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = false, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    val result = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodePropertyLate(offset, "prop", "x.prop")(xProp), literalInt(42))(pos)

    result should equal(ProduceResult(Selection(Seq(newPredicate), allNodes), Seq("x")))
    lookup(result.id) should equal(slots)
  }

  test("reading property key when the token does not exist at compile time") {
    // match (a)-[r1]->(b) where r.prop = 42
    // given
    val node1 = "a"
    val node2 = "b"
    val edge = "r"
    val argument = Argument(Set(node1, node2, edge))
    val predicate = Equals(prop("r", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.
      newLong("a", nullable = false, CTNode).
      newLong("b", nullable = false, CTNode).
      newLong("r", nullable = false, CTRelationship)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup)

    result should equal(Selection(Seq(Equals(RelationshipPropertyLate(2, "prop", "r.prop")(rProp), literalInt(42))(pos)), argument))
    lookup(result.id) should equal(slots)
  }

  test("projection with map lookup MATCH (n) RETURN n.prop") {
    // given
    val node = Variable("n")(pos)
    val allNodes = AllNodesScan(node.name, Set.empty)
    val projection = Projection(allNodes, Map("n.prop" -> prop("n", "prop")))
    val produceResult = ProduceResult(projection, Seq("n.prop"))
    val nodeOffset = 0
    val slots = SlotConfiguration.empty.
      newLong("n", nullable = false, CTNode).
      newReference("n.prop", nullable = true, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(projection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)

    //when
    val result = rewriter(produceResult, lookup)

    //then
    val newProjection = Projection(allNodes, Map("n.prop" -> NodePropertyLate(nodeOffset, "prop", "n.prop")(nProp)))
    result should equal(
      ProduceResult(newProjection, Seq("n.prop")))
    lookup(result.id) should equal(slots)
  }

  test("rewriting variable should always work, even if Variable is not part of a bigger tree") {
    // given
    val leaf = NodeByLabelScan("x", LabelName("label")(pos), Set.empty)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))
    val tokenContext = mock[TokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = false, CTNode).
      newReference("x.propertyKey", nullable = true, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(leaf.id, slots)
    lookup.set(projection.id, slots)

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val resultPlan = rewriter(projection, lookup)

    // then
    resultPlan should equal(
      Projection(leaf, Map(
        "x" -> NodeFromSlot(0, "x"),
        "x.propertyKey" -> NodeProperty(slots.getLongOffsetFor("x"), tokenId, "x.propertyKey")(propFor("x", "propertyKey"))
      ))
    )
  }

  test("make sure to handle nullable nodes correctly") {
    // given
    val leaf = NodeByLabelScan("x", LabelName("label")(pos), Set.empty)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))
    val tokenContext = mock[TokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = true, CTNode).
      newReference("x.propertyKey", nullable = true, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(leaf.id, slots)
    lookup.set(projection.id, slots)

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val resultPlan = rewriter(projection, lookup)

    // then
    val nodeOffset = slots.getLongOffsetFor("x")
    resultPlan should equal(
      Projection(leaf, Map(
        "x" -> NullCheckVariable(0, NodeFromSlot(0, "x")),
        "x.propertyKey" -> NullCheckProperty(nodeOffset, NodeProperty(nodeOffset, tokenId, "x.propertyKey")(propFor("x", "propertyKey")))
      ))
    )
  }

  test("argument on two sides of Apply") {
    val sr1 = Argument()
    val sr2 = Argument()
    val pr1A = Projection(sr1, Map("x" -> literalInt(42)))
    val pr1B = Projection(pr1A, Map("xx" -> varFor("x")))
    val pr2 = Projection(sr2, Map("y" -> literalInt(666)))
    val apply = Apply(pr1B, pr2)

    val lhsPipeline = SlotConfiguration.empty.
      newReference("x", nullable = true, CTAny).
      newReference("xx", nullable = true, CTAny)

    val rhsPipeline = SlotConfiguration.empty.
      newReference("y", nullable = true, CTAny)


    val lookup = new SlotConfigurations
    lookup.set(sr1.id, lhsPipeline)
    lookup.set(pr1A.id, lhsPipeline)
    lookup.set(pr1B.id, lhsPipeline)
    lookup.set(sr2.id, rhsPipeline)
    lookup.set(pr2.id, rhsPipeline)
    lookup.set(apply.id, rhsPipeline)

    // when
    val rewriter = new SlottedRewriter(mock[TokenContext])
    val resultPlan = rewriter(apply, lookup)

    // then

    val pr1BafterRewrite = Projection(pr1A, Map("xx" -> ReferenceFromSlot(0, "x")))
    val applyAfterRewrite = Apply(pr1BafterRewrite, pr2)

    resultPlan should equal(
      applyAfterRewrite
    )

    lookup(resultPlan.id) should equal(rhsPipeline)
    lookup(sr1.id) should equal(lhsPipeline)
    lookup(sr2.id) should equal(rhsPipeline)
  }

  test("ValueHashJoin needs to execute expressions with two different slots") {
    // MATCH (a:labelA), (b:labelB) WHERE a.prop = b.prop
    val leafA = NodeByLabelScan("a", LabelName("labelA")(pos), Set.empty)
    val leafB = NodeByLabelScan("b", LabelName("labelB")(pos), Set.empty)

    val lhsExp = prop("a", "prop")
    val rhsExp = prop("b", "prop")
    val join = ValueHashJoin(leafA, leafB, Equals(lhsExp, rhsExp)(pos))


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
    val lookup = new SlotConfigurations
    lookup.set(leafA.id, lhsPipeline)
    lookup.set(leafB.id, rhsPipeline)
    lookup.set(join.id, joinPipeline)
    val resultPlan = rewriter(join, lookup)

    // then
    val lhsExpAfterRewrite = NodeProperty(0, tokenId, "a.prop")(aProp)
    val rhsExpAfterRewrite = NodeProperty(0, tokenId, "b.prop")(bProp) // Same offsets, but on different contexts
    val joinAfterRewrite = ValueHashJoin(leafA, leafB, Equals(lhsExpAfterRewrite, rhsExpAfterRewrite)(pos))

    resultPlan should equal(
      joinAfterRewrite
    )

    lookup(resultPlan.id) should equal(joinPipeline)
    lookup(leafA.id) should equal(lhsPipeline)
    lookup(leafB.id) should equal(rhsPipeline)
  }

  test("selection with null checks against a primitive LongSlot") {
    // given
    val allNodes = AllNodesScan("x", Set.empty)
    val predicate = IsNull(varFor("x"))(pos)
    val selection = Selection(Seq(predicate), allNodes)
    val produceResult = ProduceResult(selection, Seq("x"))

    val offset = 0
    val slots = SlotConfiguration.empty.
      newLong("x", nullable = true, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup)

    // then
    val newPredicate = IsPrimitiveNull(offset)
    result should equal(ProduceResult(Selection(Seq(newPredicate), allNodes), Seq("x")))
    lookup(result.id) should equal(slots)
  }

  test("selection between two references") {
    // given
    val arg = Argument(Set("x", "z"))
    val predicate1 = Equals(varFor("x"), varFor("z"))(pos)
    val predicate2 = Not(Equals(varFor("x"), varFor("z"))(pos))(pos)
    val selection = Selection(Seq(predicate1, predicate2), arg)
    val produceResult = ProduceResult(selection, Seq("x", "z"))

    val offsetX = 0
    val offsetZ = 1
    val slots = SlotConfiguration.empty.
      newReference("x", nullable = true, CTAny).
      newReference("z", nullable = true, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(arg.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[TokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup)

    // then
    val newPred1 = Equals(ReferenceFromSlot(offsetX, "x"), ReferenceFromSlot(offsetZ, "z"))(pos)
    val newPred2 = Not(Equals(ReferenceFromSlot(offsetX, "x"), ReferenceFromSlot(offsetZ, "z"))(pos))(pos)
    result should equal(ProduceResult(Selection(Seq(newPred1, newPred2), arg), Seq("x", "z")))
    lookup(result.id) should equal(slots)
  }

  test("should be able to rewrite expressions declared as Variable or Property") {
    // given
    val arg = Argument()
    val predicate = AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(LessThan(literalInt(42), varFor("z"))(pos)))
    val selection = Selection(Seq(predicate), arg)

    val offsetN = 0
    val offsetZ = 0
    val slots = SlotConfiguration.empty.
      newLong("n", nullable = true, CTNode).
      newReference("z", nullable = false, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(arg.id, SlotConfiguration.empty)
    lookup.set(selection.id, slots)
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
    result should equal(Selection(Seq(newPred), arg))
  }
}
