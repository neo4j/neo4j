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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ast.{IdFromSlot, NodeProperty, PrimitiveEquals}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality, CardinalityEstimation, IdName, PlannerQuery}

class RegisteredRewriterTest extends CypherFunSuite with AstConstructionTestSupport {
  private val solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality(1))

  private def nodeAt(offset: Int) = LongSlot(offset, nullable = false, typ = CTNode)
  private def edgeAt(offset: Int) = LongSlot(offset, nullable = false, typ = CTNode)

  test("selection with property comparison MATCH (n) WHERE n.prop > 42 RETURN n") {
    val allNodes = AllNodesScan(IdName("x"), Set.empty)(solved)
    val predicate = GreaterThan(prop("x", "prop"), literalInt(42))(pos)
    val selection = Selection(Seq(predicate), allNodes)(solved)
    val produceResult = ProduceResult(Seq("x"), selection)
    val offset = 0
    val pipeline = PipelineInformation(Map("x" -> LongSlot(offset, nullable = false, typ = CTNode)), 1, 0)
    val lookup: Map[LogicalPlan, PipelineInformation] = Map(
      allNodes -> pipeline,
      selection -> pipeline,
      produceResult -> pipeline)
    val tokenContext = mock[TokenContext]
    val tokenId = 666
    when(tokenContext.getOptPropertyKeyId("prop")).thenReturn(Some(tokenId))
    val rewriter = new RegisteredRewriter(tokenContext)
    val (result, newLookup) = rewriter(produceResult, lookup)

    val newPredicate = GreaterThan(NodeProperty(offset, tokenId), literalInt(42))(pos)

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
    val argument = Argument(Set(node1,node2,node3,rel1,rel2))(solved)()
    val predicate = Not(Equals(varFor("r1"), varFor("r2"))(pos))(pos)
    val selection = Selection(Seq(predicate), argument)(solved)
    val pipelineInformation = PipelineInformation(Map(
      "a" -> nodeAt(0),
      "b" -> nodeAt(1),
      "r1" -> edgeAt(2),
      "c" -> nodeAt(3),
      "r2" -> edgeAt(4)
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

}
