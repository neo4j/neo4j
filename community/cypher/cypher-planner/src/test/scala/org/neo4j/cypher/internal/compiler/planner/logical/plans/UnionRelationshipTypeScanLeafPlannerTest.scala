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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.common
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.unionRelationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class UnionRelationshipTypeScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("simple outgoing directed type scan") {
    // given
    val context = planningContext()
    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      DirectedUnionRelationshipTypesScan(
        v"r",
        v"a",
        Seq(relTypeName("R"), relTypeName("S")),
        v"b",
        Set.empty,
        IndexOrderNone
      )
    ))
  }

  test("simple incoming directed type scan") {
    // given
    val context = planningContext()
    // (a)<-[:R|S]-(b)
    val qg = pattern(v"r", v"a", v"b", INCOMING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      DirectedUnionRelationshipTypesScan(
        v"r",
        v"b",
        Seq(relTypeName("R"), relTypeName("S")),
        v"a",
        Set.empty,
        IndexOrderNone
      )
    ))
  }

  test("simple undirected type scan") {
    // given
    val context = planningContext()
    // (a)-[:R|S]-(b)
    val qg = pattern(v"r", v"a", v"b", BOTH, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      UndirectedUnionRelationshipTypesScan(
        v"r",
        v"a",
        Seq(relTypeName("R"), relTypeName("S")),
        v"b",
        Set.empty,
        IndexOrderNone
      )
    ))
  }

  test("should not scan if single types") {
    // given
    val context = planningContext()
    // (a)-[:R]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not scan if variable length pattern") {
    // given
    val context = planningContext()
    // (a)-[:R|S*]->(b)
    val qg = varPattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not plan type scan for skipped ids") {
    // given
    val context = planningContext()
    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // then
    unionRelationshipTypeScanLeafPlanner(Set(v"r"))(qg, InterestingOrderConfig.empty, context) should be(empty)
    unionRelationshipTypeScanLeafPlanner(Set(v"a"))(qg, InterestingOrderConfig.empty, context) should be(empty)
    unionRelationshipTypeScanLeafPlanner(Set(v"b"))(qg, InterestingOrderConfig.empty, context) should be(empty)
  }

  test("should not plan type scan when rel id is in arguments") {
    // given
    // (a)-[:R|S]->(b)
    val context = planningContext()
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // then
    unionRelationshipTypeScanLeafPlanner(Set.empty)(
      qg.withArgumentIds(Set(v"r")),
      InterestingOrderConfig.empty,
      context
    ) should be(empty)
  }

  test("should not plan type scan if no type index") {
    // given
    val context = planningContext(typeScanEnabled = false)

    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("outgoing directed relationship type scan with required ascending order") {
    // given
    val context = planningContext()
    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(
      qg,
      InterestingOrderConfig(
        InterestingOrder(RequiredOrderCandidate(Seq(ColumnOrder(v"r", ascending = true))))
      ),
      context
    )

    // then
    resultPlans should equal(Set(
      DirectedUnionRelationshipTypesScan(
        v"r",
        v"a",
        Seq(relTypeName("R"), relTypeName("S")),
        v"b",
        Set.empty,
        IndexOrderAscending
      )
    ))
  }

  test("outgoing directed relationship type scan with required descending order") {
    // given
    val context = planningContext()
    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(
      qg,
      InterestingOrderConfig(
        InterestingOrder(RequiredOrderCandidate(Seq(ColumnOrder(v"r", ascending = false))))
      ),
      context
    )

    // then
    resultPlans should equal(Set(
      DirectedUnionRelationshipTypesScan(
        v"r",
        v"a",
        Seq(relTypeName("R"), relTypeName("S")),
        v"b",
        Set.empty,
        IndexOrderDescending
      )
    ))
  }

  test("outgoing directed relationship type scan with interesting order") {
    // given
    val context = planningContext()
    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(
      qg,
      InterestingOrderConfig(
        InterestingOrder(
          RequiredOrderCandidate(Seq.empty),
          Seq(InterestingOrderCandidate(Seq(ColumnOrder(v"r", ascending = false))))
        )
      ),
      context
    )

    // then
    resultPlans should equal(Set(
      DirectedUnionRelationshipTypesScan(
        v"r",
        v"a",
        Seq(relTypeName("R"), relTypeName("S")),
        v"b",
        Set.empty,
        IndexOrderDescending
      )
    ))
  }

  test("outgoing directed relationship type scan with required and interesting order") {
    // given
    val context = planningContext()
    // (a)-[:R|S]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R", "S")

    // when
    val resultPlans = unionRelationshipTypeScanLeafPlanner(Set.empty)(
      qg,
      InterestingOrderConfig(
        InterestingOrder(
          RequiredOrderCandidate(Seq(ColumnOrder(v"r", ascending = true))),
          Seq(InterestingOrderCandidate(Seq(ColumnOrder(v"r", ascending = false))))
        )
      ),
      context
    )

    // then
    resultPlans should equal(Set(
      DirectedUnionRelationshipTypesScan(
        v"r",
        v"a",
        Seq(relTypeName("R"), relTypeName("S")),
        v"b",
        Set.empty,
        IndexOrderAscending
      )
    ))
  }

  private def pattern(
    name: LogicalVariable,
    from: LogicalVariable,
    to: LogicalVariable,
    direction: SemanticDirection,
    types: String*
  ) =
    QueryGraph(
      patternNodes = Set(name, from, to),
      patternRelationships =
        Set(PatternRelationship(
          name,
          (from, to),
          direction,
          types.map(relTypeName(_)),
          SimplePatternLength
        ))
    )

  private def varPattern(
    name: LogicalVariable,
    from: LogicalVariable,
    to: LogicalVariable,
    direction: SemanticDirection,
    types: String*
  ) =
    QueryGraph(
      patternNodes = Set(name, from, to),
      patternRelationships =
        Set(PatternRelationship(
          name,
          (from, to),
          direction,
          types.map(relTypeName(_)),
          VarPatternLength(1, None)
        ))
    )

  def planningContext(typeScanEnabled: Boolean = true): LogicalPlanningContext = {
    val planContext = newMockedPlanContext()
    val tokenIndex =
      if (typeScanEnabled) Some(TokenIndexDescriptor(common.EntityType.RELATIONSHIP, IndexOrderCapability.BOTH))
      else None
    when(planContext.relationshipTokenIndex).thenReturn(tokenIndex)
    newMockedLogicalPlanningContext(planContext = planContext, semanticTable = newMockedSemanticTable)
  }

}
