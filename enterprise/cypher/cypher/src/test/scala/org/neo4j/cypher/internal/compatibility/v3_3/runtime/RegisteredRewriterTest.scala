/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ast._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality, CardinalityEstimation, IdName, PlannerQuery}

class RegisteredRewriterTest extends CypherFunSuite with AstConstructionTestSupport {
  private val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  private def nodeAt(offset: Int, name: String) = LongSlot(offset, nullable = false, typ = CTNode, name = name)
  private def edgeAt(offset: Int, name: String) = LongSlot(offset, nullable = false, typ = CTRelationship, name = name)

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n") {
    val allNodes = AllNodesScan(IdName("x"), Set.empty)(solved)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)(solved)
    val produceResult = ProduceResult(Seq("x"), selection)
    val offset = 0
    val pipeline = PipelineInformation(Map("x" -> LongSlot(offset, nullable = false, typ = CTNode, "x")), 1, 0)
    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      allNodes -> pipeline,
      selection -> pipeline,
      produceResult -> pipeline)
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new RegisteredRewriter(tokenContext)
    val (result, newLookup) = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodeProperty(offset, tokenId, "x.prop"), literalInt(42))(pos)

    result should equal(ProduceResult(Seq("x"), Selection(Seq(newPredicate), allNodes)(solved)))
    newLookup(result) should equal(pipeline)
  }

  test("comparing two relationship ids simpler") {
    // match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = IdName("a")
    val node2 = IdName("b")
    val node3 = IdName("c")
    val rel1 = IdName("r1")
    val rel2 = IdName("r2")
    val argument = Argument(Set(node1, node2, node3, rel1, rel2))(solved)()
    val predicate = Not(Equals(varFor("r1"), varFor("r2"))(pos))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    val pipelineInformation = PipelineInformation(Map(
      "a" -> nodeAt(0, "a"),
      "b" -> nodeAt(1, "b"),
      "r1" -> edgeAt(2, "r1"),
      "c" -> nodeAt(3, "c"),
      "r2" -> edgeAt(4, "r2")
    ), numberOfLongs = 5, numberOfReferences = 0)

    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      argument -> pipelineInformation,
      selection -> pipelineInformation
    )
    val tokenContext = mock[TokenContext]
    val rewriter = new RegisteredRewriter(tokenContext)

    // when
    val (result, newLookup) = rewriter(selection, lookup)

    // then
    result should equal(Selection(Seq(Not(PrimitiveEquals(IdFromSlot(2), IdFromSlot(4)))(pos)), argument)(solved))
    newLookup(result) should equal(pipelineInformation)
  }

  test("return nullable node") {
    // match optional (a) return (a)
    // given
    val node1 = IdName("a")
    val argument = AllNodesScan(node1, Set.empty)(solved)
    val predicate = Equals(prop("a", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    val pipelineInformation = PipelineInformation(Map(
      "a" -> LongSlot(0, nullable = true, typ = CTNode, name = "a")
    ), numberOfLongs = 1, numberOfReferences = 0)

    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      argument -> pipelineInformation,
      selection -> pipelineInformation
    )
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new RegisteredRewriter(tokenContext)

    // when
    val (result, newLookup) = rewriter(selection, lookup)

    // then
    val expectedPredicate = Equals(NullCheck(0, NodeProperty(0, 666, "a.prop")), literalInt(42))(pos)
    result should equal(Selection(Seq(expectedPredicate), argument)(solved))
    newLookup(result) should equal(pipelineInformation)
  }

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n when token is unknown") {
    val allNodes = AllNodesScan(IdName("x"), Set.empty)(solved)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)(solved)
    val produceResult = ProduceResult(Seq("x"), selection)
    val offset = 0
    val pipeline = PipelineInformation(Map("x" -> LongSlot(offset, nullable = false, typ = CTNode, "x")), 1, 0)
    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      allNodes -> pipeline,
      selection -> pipeline,
      produceResult -> pipeline)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new RegisteredRewriter(tokenContext)
    val (result, newLookup) = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodePropertyLate(offset, "prop", "x.prop"), literalInt(42))(pos)

    result should equal(ProduceResult(Seq("x"), Selection(Seq(newPredicate), allNodes)(solved)))
    newLookup(result) should equal(pipeline)
  }

  test("reading property key when the token does not exist at compile time") {
    // match (a)-[r1]->b-[r2]->(c) where not(r1 = r2)
    // given
    val node1 = IdName("a")
    val node2 = IdName("b")
    val edge = IdName("r")
    val argument = Argument(Set(node1, node2, edge))(solved)()
    val predicate = Equals(prop("r", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    val pipelineInformation = PipelineInformation(Map(
      "a" -> nodeAt(0, "a"),
      "b" -> nodeAt(1, "b"),
      "r" -> edgeAt(2, "r")
    ), numberOfLongs = 3, numberOfReferences = 0)

    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      argument -> pipelineInformation,
      selection -> pipelineInformation
    )
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new RegisteredRewriter(tokenContext)

    // when
    val (result, newLookup) = rewriter(selection, lookup)

    result should equal(Selection(Seq(Equals(RelationshipPropertyLate(2, "prop", "r.prop"), literalInt(42))(pos)), argument)(solved))
    newLookup(result) should equal(pipelineInformation)
  }

  test("projection with map lookup MATCH (n) RETURN n.prop") {
    // given
    val node = Variable("n")(pos)
    val allNodes = AllNodesScan(IdName.fromVariable(node), Set.empty)(solved)
    val projection = Projection(allNodes, Map("n.prop" -> prop("n", "prop")))(solved)
    val produceResult = ProduceResult(Seq("n.prop"), projection)
    val nodeOffset = 0
    val propOffset = 0
    val pipeline = PipelineInformation(Map(
      "n" -> LongSlot(nodeOffset, nullable = false, typ = CTNode, "n"),
      "n.prop" -> RefSlot(propOffset, nullable = true, typ = CTAny, "n.prop")),
      1, 1)
    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      allNodes -> pipeline,
      projection -> pipeline,
      produceResult -> pipeline)
    val tokenContext = mock[TokenContext]
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(None)
    val rewriter = new RegisteredRewriter(tokenContext)

    //when
    val (result, newLookup) = rewriter(produceResult, lookup)

    //then
    val newProjection = Projection(allNodes, Map("n.prop" -> NodePropertyLate(nodeOffset, "prop", "n.prop")))(solved)
    result should equal(
      ProduceResult(Seq("n.prop"), newProjection))
    newLookup(result) should equal(pipeline)
    newLookup(newProjection) should equal(pipeline)
  }

  test("rewriting variable should always work, even if Variable is not part of a bigger tree") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)
    val tokenContext = mock[TokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val pipeline = PipelineInformation.empty.
      newLong("x", nullable = false, CTNode).
      newReference("x.propertyKey", nullable = true, CTAny)

    // when
    val rewriter = new RegisteredRewriter(tokenContext)
    val (resultPlan, newLookup) = rewriter(projection, Map(leaf -> pipeline, projection -> pipeline))

    // then
    resultPlan should equal(
      Projection(leaf, Map(
        "x.propertyKey" -> NodeProperty(pipeline.getLongOffsetFor("x"), tokenId, "x.propertyKey")
      ))(solved)
    )
  }

  test("make sure to handle nullable nodes correctly") {
    // given
    val leaf = NodeByLabelScan(IdName("x"), LabelName("label")(pos), Set.empty)(solved)
    val projection = Projection(leaf, Map("x" -> varFor("x"), "x.propertyKey" -> prop("x", "propertyKey")))(solved)
    val tokenContext = mock[TokenContext]
    val tokenId = 2
    when(tokenContext.getOptPropertyKeyId("propertyKey")).thenReturn(Some(tokenId))
    val pipeline = PipelineInformation.empty.
      newLong("x", nullable = true, CTNode).
      newReference("x.propertyKey", nullable = true, CTAny)

    // when
    val rewriter = new RegisteredRewriter(tokenContext)
    val (resultPlan, newLookup) = rewriter(projection, Map(leaf -> pipeline, projection -> pipeline))

    // then
    val nodeOffset = pipeline.getLongOffsetFor("x")
    resultPlan should equal(
      Projection(leaf, Map(
        "x.propertyKey" -> NullCheck(nodeOffset, NodeProperty(nodeOffset, tokenId, "x.propertyKey"))
      ))(solved)
    )
  }

}
