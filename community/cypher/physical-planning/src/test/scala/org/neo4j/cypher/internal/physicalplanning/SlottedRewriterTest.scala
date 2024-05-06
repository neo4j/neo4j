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
package org.neo4j.cypher.internal.physicalplanning

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.SlotConfigurations
import org.neo4j.cypher.internal.physicalplanning.PhysicalPlanningAttributes.TrailPlans
import org.neo4j.cypher.internal.physicalplanning.SlottedRewriterTest.runtimeVarFor
import org.neo4j.cypher.internal.physicalplanning.ast.IdFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.IsPrimitiveNull
import org.neo4j.cypher.internal.physicalplanning.ast.NodeFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.NodeProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyExistsLate
import org.neo4j.cypher.internal.physicalplanning.ast.NodePropertyLate
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheck
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckReferenceProperty
import org.neo4j.cypher.internal.physicalplanning.ast.NullCheckVariable
import org.neo4j.cypher.internal.physicalplanning.ast.PrimitiveEquals
import org.neo4j.cypher.internal.physicalplanning.ast.ReferenceFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipFromSlot
import org.neo4j.cypher.internal.physicalplanning.ast.RelationshipPropertyLate
import org.neo4j.cypher.internal.physicalplanning.ast.SlottedCachedPropertyWithPropertyToken
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

object SlottedRewriterTest {

  def runtimeVarFor(name: String, slots: SlotConfiguration): LogicalVariable = {
    slots(name) match {
      case LongSlot(offset, false, CTNode) =>
        NodeFromSlot(offset, name)
      case LongSlot(offset, false, CTRelationship) =>
        RelationshipFromSlot(offset, name)
      case LongSlot(offset, true, CTNode) =>
        NullCheckVariable(offset, NodeFromSlot(offset, name))
      case LongSlot(offset, true, CTRelationship) =>
        NullCheckVariable(offset, RelationshipFromSlot(offset, name))
      case RefSlot(offset, _, _) =>
        ReferenceFromSlot(offset, name)
    }
  }
}

class SlottedRewriterTest extends CypherFunSuite with AstConstructionTestSupport {
  implicit val idGen: SequentialIdGen = new SequentialIdGen()
  private val xProp = prop("x", "prop")
  private val xPropKey = prop("x", "propertyKey")
  private val aProp = prop("a", "prop")
  private val bProp = prop("b", "prop")
  private val nProp = prop("n", "prop")
  private val rProp = prop("r", "prop")

  test("checking property existence using IS NULL on a nullable node") {
    // OPTIONAL MATCH (n) WHERE n.prop IS NULL
    // given
    val argument = Argument(Set(varFor("n")))
    val predicate = isNull(nProp)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("n", nullable = true, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    // when
    val result = rewriter(selection, lookup, new TrailPlans)
    result should equal(
      Selection(
        Seq(
          or(
            IsPrimitiveNull(0),
            not(
              NodePropertyExistsLate(0, "prop", "n.prop")(nProp)
            )
          )
        ),
        Argument(Set(runtimeVarFor("n", slots)))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("checking property existence using IS NULL on a node") {
    // MATCH (n) WHERE n.prop IS NULL
    // given
    val argument = Argument(Set(varFor("n")))
    val predicate = isNull(nProp)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    // when
    val result = rewriter(selection, lookup, new TrailPlans)
    result should equal(
      Selection(
        Seq(
          not(
            NodePropertyExistsLate(0, "prop", "n.prop")(nProp)
          )
        ),
        Argument(Set(runtimeVarFor("n", slots)))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("checking property existence using IS NOT NULL on a nullable node") {
    // OPTIONAL MATCH (n) WHERE n.prop IS NOT NULL
    // given
    val argument = Argument(Set(varFor("n")))
    val predicate = isNotNull(nProp)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("n", nullable = true, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    // when
    val result = rewriter(selection, lookup, new TrailPlans)
    result should equal(
      Selection(
        Seq(
          and(
            not(
              IsPrimitiveNull(0)
            ),
            NodePropertyExistsLate(0, "prop", "n.prop")(nProp)
          )
        ),
        Argument(Set(runtimeVarFor("n", slots)))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("checking property existence using IS NOT NULL on a node") {
    // MATCH (n) WHERE n.prop IS NOT NULL
    // given
    val argument = Argument(Set(varFor("n")))
    val predicate = isNotNull(nProp)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("n", nullable = false, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    // when
    val result = rewriter(selection, lookup, new TrailPlans)
    result should equal(
      Selection(
        Seq(
          NodePropertyExistsLate(0, "prop", "n.prop")(nProp)
        ),
        Argument(Set(runtimeVarFor("n", slots)))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n") {
    val allNodes = AllNodesScan(varFor("x"), Set.empty)
    val predicate = greaterThan(xProp, literalInt(42))
    val selection = Selection(Seq(predicate), allNodes)
    val produceResult = ProduceResult.withNoCachedProperties(selection, Seq(varFor("x")))
    val offset = 0
    val slots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)
    val result = rewriter(produceResult, lookup, new TrailPlans)

    val newPredicate = greaterThan(NodeProperty(offset, tokenId, "x.prop")(xProp), literalInt(42))

    result should equal(
      plans.ProduceResult.withNoCachedProperties(
        source = Selection(Seq(newPredicate), AllNodesScan(runtimeVarFor("x", slots), Set.empty)),
        columns = Seq(runtimeVarFor("x", slots))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("comparing two relationship ids simpler") {
    // match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = varFor("a")
    val node2 = varFor("b")
    val node3 = varFor("c")
    val rel1 = varFor("r1")
    val rel2 = varFor("r2")
    val argVarSet: Set[LogicalVariable] = Set(node1, node2, node3, rel1, rel2)
    val argument = Argument(argVarSet)
    val predicate = not(equals(rel1, rel2))
    val selection = Selection(Seq(predicate), argument)
    val slots =
      SlotConfiguration.empty.newLong("a", nullable = false, CTNode)
        .newLong("b", nullable = false, CTNode)
        .newLong("r1", nullable = false, CTRelationship)
        .newLong("c", nullable = false, CTNode)
        .newLong("r2", nullable = false, CTRelationship)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup, new TrailPlans)

    // then
    val argVars: Set[LogicalVariable] = argVarSet.map(v => runtimeVarFor(v.name, slots))
    result should equal(Selection(
      Seq(not(PrimitiveEquals(IdFromSlot(2), IdFromSlot(4)))),
      Argument(argVars)
    ))
    lookup(result.id) should equal(slots)
  }

  test("comparing two relationship ids simpler when they are null") {
    // optional match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = varFor("a")
    val node2 = varFor("b")
    val node3 = varFor("c")
    val rel1 = varFor("r1")
    val rel2 = varFor("r2")
    val argVarSet: Set[LogicalVariable] = Set(node1, node2, node3, rel1, rel2)
    val argument = Argument(argVarSet)
    val predicate = not(equals(rel1, rel2))
    val selection = Selection(Seq(predicate), argument)
    val slots =
      SlotConfiguration.empty.newLong("a", nullable = false, CTNode).newLong("b", nullable = false, CTNode).newLong(
        "r1",
        nullable = true,
        CTNode
      ).newLong("c", nullable = false, CTNode).newLong("r2", nullable = true, CTNode)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup, new TrailPlans)

    // then
    val argVars: Set[LogicalVariable] = argVarSet.map(v => runtimeVarFor(v.name, slots))
    val rewrittenPredicate =
      NullCheck(
        2,
        NullCheck(
          4,
          not(
            PrimitiveEquals(
              IdFromSlot(2),
              IdFromSlot(4)
            )
          )
        )
      )
    result should equal(Selection(
      Seq(rewrittenPredicate),
      Argument(argVars)
    ))
    lookup(result.id) should equal(slots)
  }

  test("comparing different types must check nulls before returning shortcut") {
    // optional match (a)-[r1]->() where not(r1 = a)
    // given
    val node1 = varFor("a")
    val node2 = varFor("b")
    val rel = varFor("r")
    val argVarSet: Set[LogicalVariable] = Set(node1, node2, rel)
    val argument = Argument(argVarSet)
    val predicate = equals(rel, node1)
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("a", nullable = true, CTNode).newLong(
      "b",
      nullable = false,
      CTNode
    ).newLong("r", nullable = true, CTRelationship)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup, new TrailPlans)

    // then since we are doing a not(t1 = t2), we shortcut to True if neither value is null
    val rewrittenPredicate =
      NullCheck(2, NullCheck(0, falseLiteral))

    val argVars = argVarSet.map(v => runtimeVarFor(v.name, slots))
    result should equal(Selection(
      Seq(rewrittenPredicate),
      Argument(argVars)
    ))
    lookup(result.id) should equal(slots)
  }

  test("return nullable node") {
    // match optional (a) return (a)
    // given
    val argument = AllNodesScan(varFor("a"), Set.empty)
    val predicate = equals(aProp, literalInt(42))
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("a", nullable = true, CTNode)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup, new TrailPlans)

    // then
    val expectedPredicate = equals(NullCheckProperty(0, NodeProperty(0, 666, "a.prop")(aProp)), literalInt(42))
    result should equal(Selection(Seq(expectedPredicate), AllNodesScan(runtimeVarFor("a", slots), Set.empty)))
    lookup(result.id) should equal(slots)
  }

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n when token is unknown") {
    val allNodes = AllNodesScan(varFor("x"), Set.empty)
    val predicate = greaterThan(xProp, literalInt(42))
    val selection = Selection(Seq(predicate), allNodes)
    val produceResult = plans.ProduceResult.withNoCachedProperties(selection, Seq(varFor("x")))
    val offset = 0
    val slots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)
    val result = rewriter(produceResult, lookup, new TrailPlans)

    val newPredicate = greaterThan(NodePropertyLate(offset, "prop", "x.prop")(xProp), literalInt(42))

    result should equal(
      plans.ProduceResult.withNoCachedProperties(
        Selection(Seq(newPredicate), AllNodesScan(runtimeVarFor("x", slots), Set.empty)),
        Seq(runtimeVarFor("x", slots))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("reading property key when the token does not exist at compile time") {
    // match (a)-[r1]->(b) where r.prop = 42
    // given
    val node1 = varFor("a")
    val node2 = varFor("b")
    val edge = varFor("r")
    val argVarSet: Set[LogicalVariable] = Set(node1, node2, edge)
    val argument = Argument(argVarSet)
    val predicate = equals(rProp, literalInt(42))
    val selection = Selection(Seq(predicate), argument)
    val slots = SlotConfiguration.empty.newLong("a", nullable = false, CTNode).newLong(
      "b",
      nullable = false,
      CTNode
    ).newLong("r", nullable = false, CTRelationship)

    val lookup = new SlotConfigurations
    lookup.set(argument.id, slots)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup, new TrailPlans)

    result should equal(Selection(
      Seq(equals(RelationshipPropertyLate(2, "prop", "r.prop")(rProp), literalInt(42))),
      Argument(argVarSet.map(v => runtimeVarFor(v.name, slots)))
    ))
    lookup(result.id) should equal(slots)
  }

  test("projection with map lookup MATCH (n) RETURN n.prop") {
    // given
    val node = varFor("n")
    val allNodes = AllNodesScan(node, Set.empty)
    val projection = Projection(allNodes, Map(varFor("n.prop") -> nProp))
    val produceResult = ProduceResult.withNoCachedProperties(projection, Seq(varFor("n.prop")))
    val nodeOffset = 0
    val slots =
      SlotConfiguration.empty.newLong("n", nullable = false, CTNode).newReference("n.prop", nullable = true, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(projection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup, new TrailPlans)

    // then
    result should equal(
      ProduceResult.withNoCachedProperties(
        Projection(
          AllNodesScan(runtimeVarFor("n", slots), Set.empty),
          Map(runtimeVarFor("n.prop", slots) -> NodePropertyLate(nodeOffset, "prop", "n.prop")(nProp))
        ),
        Seq(runtimeVarFor("n.prop", slots))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("rewriting variable should always work, even if Variable is not part of a bigger tree") {
    // given
    val leaf = NodeByLabelScan(varFor("x"), labelName("label"), Set.empty, IndexOrderNone)
    val projection = Projection(leaf, Map(varFor("x") -> varFor("x"), varFor("x.propertyKey") -> xPropKey))
    val tokenContext = mock[ReadTokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val slots = SlotConfiguration.empty.newLong("x", nullable = false, CTNode).newReference(
      "x.propertyKey",
      nullable = true,
      CTAny
    )
    val lookup = new SlotConfigurations
    lookup.set(leaf.id, slots)
    lookup.set(projection.id, slots)

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val resultPlan = rewriter(projection, lookup, new TrailPlans)

    // then
    resultPlan should equal(
      Projection(
        NodeByLabelScan(runtimeVarFor("x", slots), labelName("label"), Set.empty, IndexOrderNone),
        Map(
          runtimeVarFor("x", slots) -> runtimeVarFor("x", slots),
          runtimeVarFor("x.propertyKey", slots) -> NodeProperty(slots.getLongOffsetFor("x"), tokenId, "x.propertyKey")(
            xPropKey
          )
        )
      )
    )
  }

  test("make sure to handle nullable nodes correctly") {
    // given
    val leaf = NodeByLabelScan(varFor("x"), labelName("label"), Set.empty, IndexOrderNone)
    val projection = Projection(leaf, Map(varFor("x") -> varFor("x"), varFor("x.propertyKey") -> xPropKey))
    val tokenContext = mock[ReadTokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val slots = SlotConfiguration.empty.newLong("x", nullable = true, CTNode).newReference(
      "x.propertyKey",
      nullable = true,
      CTAny
    )
    val lookup = new SlotConfigurations
    lookup.set(leaf.id, slots)
    lookup.set(projection.id, slots)

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val resultPlan = rewriter(projection, lookup, new TrailPlans)

    // then
    val nodeOffset = slots.getLongOffsetFor("x")
    resultPlan should equal(
      Projection(
        NodeByLabelScan(runtimeVarFor("x", slots), labelName("label"), Set.empty, IndexOrderNone),
        Map(
          runtimeVarFor("x", slots) -> runtimeVarFor("x", slots),
          runtimeVarFor("x.propertyKey", slots) -> NullCheckProperty(
            nodeOffset,
            NodeProperty(nodeOffset, tokenId, "x.propertyKey")(xPropKey)
          )
        )
      )
    )
  }

  test("argument on two sides of Apply") {
    val sr1 = Argument()
    val sr2 = Argument()
    val pr1A = Projection(sr1, Map(varFor("x") -> literalInt(42)))
    val pr1B = Projection(pr1A, Map(varFor("xx") -> varFor("x")))
    val pr2 = Projection(sr2, Map(varFor("y") -> literalInt(666)))
    val apply = Apply(pr1B, pr2)

    val lhsPipeline =
      SlotConfiguration.empty.newReference("x", nullable = true, CTAny).newReference("xx", nullable = true, CTAny)

    val rhsPipeline = SlotConfiguration.empty.newReference("y", nullable = true, CTAny)

    val lookup = new SlotConfigurations
    lookup.set(sr1.id, lhsPipeline)
    lookup.set(pr1A.id, lhsPipeline)
    lookup.set(pr1B.id, lhsPipeline)
    lookup.set(sr2.id, rhsPipeline)
    lookup.set(pr2.id, rhsPipeline)
    lookup.set(apply.id, rhsPipeline)

    // when
    val rewriter = new SlottedRewriter(mock[ReadTokenContext])
    val resultPlan = rewriter(apply, lookup, new TrailPlans)

    // then
    resultPlan should equal(
      Apply(
        Projection(
          Projection(Argument(), Map(runtimeVarFor("x", lhsPipeline) -> literalInt(42))),
          Map(runtimeVarFor("xx", lhsPipeline) -> ReferenceFromSlot(0, "x"))
        ),
        Projection(Argument(), Map(runtimeVarFor("y", rhsPipeline) -> literalInt(666)))
      )
    )

    lookup(resultPlan.id) should equal(rhsPipeline)
    lookup(sr1.id) should equal(lhsPipeline)
    lookup(sr2.id) should equal(rhsPipeline)
  }

  test("ValueHashJoin needs to execute expressions with two different slots") {
    // MATCH (a:labelA), (b:labelB) WHERE a.prop = b.prop
    val leafA = NodeByLabelScan(varFor("a"), labelName("labelA"), Set.empty, IndexOrderNone)
    val leafB = NodeByLabelScan(varFor("b"), labelName("labelB"), Set.empty, IndexOrderNone)

    val join = ValueHashJoin(leafA, leafB, equals(aProp, bProp))

    val lhsPipeline = SlotConfiguration.empty.newLong("a", nullable = false, CTNode)

    val rhsPipeline = SlotConfiguration.empty.newLong("b", nullable = false, CTNode)

    val joinPipeline =
      SlotConfiguration.empty.newLong("a", nullable = false, CTNode).newLong("b", nullable = false, CTNode)

    val tokenId = 666
    val tokenContext = mock[ReadTokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))

    // when
    val rewriter = new SlottedRewriter(tokenContext)
    val lookup = new SlotConfigurations
    lookup.set(leafA.id, lhsPipeline)
    lookup.set(leafB.id, rhsPipeline)
    lookup.set(join.id, joinPipeline)
    val resultPlan = rewriter(join, lookup, new TrailPlans)

    // then
    val lhsExpAfterRewrite = NodeProperty(0, tokenId, "a.prop")(aProp)
    val rhsExpAfterRewrite = NodeProperty(0, tokenId, "b.prop")(bProp) // Same offsets, but on different contexts

    resultPlan should equal(
      ValueHashJoin(
        NodeByLabelScan(runtimeVarFor("a", lhsPipeline), labelName("labelA"), Set.empty, IndexOrderNone),
        NodeByLabelScan(runtimeVarFor("b", rhsPipeline), labelName("labelB"), Set.empty, IndexOrderNone),
        equals(lhsExpAfterRewrite, rhsExpAfterRewrite)
      )
    )

    lookup(resultPlan.id) should equal(joinPipeline)
    lookup(leafA.id) should equal(lhsPipeline)
    lookup(leafB.id) should equal(rhsPipeline)
  }

  test("selection with null checks against a primitive LongSlot") {
    // given
    val allNodes = AllNodesScan(varFor("x"), Set.empty)
    val predicate = isNull(varFor("x"))
    val selection = Selection(Seq(predicate), allNodes)
    val produceResult = plans.ProduceResult.withNoCachedProperties(selection, Seq(varFor("x")))

    val offset = 0
    val slots = SlotConfiguration.empty.newLong("x", nullable = true, CTNode)
    val lookup = new SlotConfigurations
    lookup.set(allNodes.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup, new TrailPlans)

    // then
    val newPredicate = IsPrimitiveNull(offset)
    result should equal(
      plans.ProduceResult.withNoCachedProperties(
        source = Selection(Seq(newPredicate), AllNodesScan(runtimeVarFor("x", slots), Set.empty)),
        columns = Seq(runtimeVarFor("x", slots))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("selection between two references") {
    // given
    val arg = Argument(Set(varFor("x"), varFor("z")))
    val predicate1 = equals(varFor("x"), varFor("z"))
    val predicate2 = not(equals(varFor("x"), varFor("z")))
    val selection = Selection(Seq(predicate1, predicate2), arg)
    val produceResult = plans.ProduceResult.withNoCachedProperties(selection, Seq(varFor("x"), varFor("z")))

    val offsetX = 0
    val offsetZ = 1
    val slots =
      SlotConfiguration.empty.newReference("x", nullable = true, CTAny).newReference("z", nullable = true, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(arg.id, slots)
    lookup.set(selection.id, slots)
    lookup.set(produceResult.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(produceResult, lookup, new TrailPlans)

    // then
    val newPred1 = equals(ReferenceFromSlot(offsetX, "x"), ReferenceFromSlot(offsetZ, "z"))
    val newPred2 = not(equals(ReferenceFromSlot(offsetX, "x"), ReferenceFromSlot(offsetZ, "z")))
    result should equal(
      plans.ProduceResult.withNoCachedProperties(
        Selection(Seq(newPred1, newPred2), Argument(Set(runtimeVarFor("x", slots), runtimeVarFor("z", slots)))),
        Seq(runtimeVarFor("x", slots), runtimeVarFor("z", slots))
      )
    )
    lookup(result.id) should equal(slots)
  }

  test("should be able to rewrite expressions declared as Variable or Property") {
    // given
    val arg = Argument()
    val predicate = AndedPropertyInequalities(varFor("n"), nProp, NonEmptyList(lessThan(literalInt(42), varFor("z"))))
    val selection = Selection(Seq(predicate), arg)

    val offsetN = 0
    val offsetZ = 0
    val slots = SlotConfiguration.empty.newLong("n", nullable = true, CTNode).newReference("z", nullable = false, CTAny)
    val lookup = new SlotConfigurations
    lookup.set(arg.id, SlotConfiguration.empty)
    lookup.set(selection.id, slots)
    val tokenContext = mock[ReadTokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new SlottedRewriter(tokenContext)

    // when
    val result = rewriter(selection, lookup, new TrailPlans)

    // then
    val newPred = AndedPropertyInequalities(
      NullCheckVariable(0, NodeFromSlot(offsetN, "n")),
      NullCheckProperty(offsetN, NodeProperty(offsetN, 666, "n.prop")(xProp)),
      NonEmptyList(lessThan(literalInt(42), ReferenceFromSlot(offsetZ, "z")))
    )
    result should equal(Selection(Seq(newPred), arg))
  }

  test("should rewrite cached property of a nullable entity in ref slot using NullCheckReferenceProperty") {
    // given
    val arg = Argument()
    val property = CachedProperty(
      Variable("n.prop")(InputPosition.NONE),
      Variable("n")(InputPosition.NONE),
      PropertyKeyName("prop")(InputPosition.NONE),
      NODE_TYPE
    )(InputPosition.NONE)
    val properties = CacheProperties(arg, Set(property))

    val slots = SlotConfiguration.empty
      .newReference("n", nullable = true, CTAny)
      .newCachedProperty(property.runtimeKey)
    val tokenContext = mock[ReadTokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))

    val rewriter = new SlottedRewriter(tokenContext)

    val lookup = new SlotConfigurations
    lookup.set(arg.id, SlotConfiguration.empty)
    lookup.set(properties.id, slots)

    // when
    val result = rewriter(properties, lookup, new TrailPlans)

    // then
    result should equal {
      CacheProperties(
        arg,
        Set(NullCheckReferenceProperty(
          offset = 0,
          inner = SlottedCachedPropertyWithPropertyToken(
            entityName = "n.prop",
            propertyKey = PropertyKeyName("prop")(InputPosition.NONE),
            offset = 0,
            offsetIsForLongSlot = false,
            propToken = 666,
            cachedPropertyOffset = 1,
            entityType = NODE_TYPE,
            nullable = true
          )
        ))
      )
    }
  }
}
