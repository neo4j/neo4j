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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.verification.VerificationMode
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlanner.CacheKeyInner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlanner.CacheKeyOuter
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.QPPInnerPlannerOps
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.QPPInnerPlansOps
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.`(a) ((n)-[r]->(m))+ (b)`
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.`(c) ((x)-[r]->(y))+ (d)`
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.dummyPlan
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.fromLeft
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.fromRight
import org.neo4j.cypher.internal.compiler.planner.logical.idp.CacheBackedQPPInnerPlannerTest.mockPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQPPPredicates.ExtractedPredicates
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.jdk.CollectionConverters.MapHasAsScala

class CacheBackedQPPInnerPlannerTest extends CypherFunSuite {

  test("Should plan 1 time even for identical requests") {
    val planner = mockPlanner()
    val cache = new CacheBackedQPPInnerPlanner(planner)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)

    planner.called(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)(times(1))
  }

  test("Should cache 1 plan for identical requests") {
    val planner = mockPlanner()
    val cache = new CacheBackedQPPInnerPlanner(planner)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)

    cache.entries(`(a) ((n)-[r]->(m))+ (b)`, fromLeft) should contain theSameElementsAs Map(
      CacheKeyInner(Set.empty) -> dummyPlan
    )
  }

  test("Should plan 1 time per identical request") {
    val planner = mockPlanner()
    val cache = new CacheBackedQPPInnerPlanner(planner)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromRight)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromRight)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(c) ((x)-[r]->(y))+ (d)`, fromLeft)

    planner.called(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)(times(1))
    planner.called(`(a) ((n)-[r]->(m))+ (b)`, fromRight)(times(1))
    planner.called(`(c) ((x)-[r]->(y))+ (d)`, fromLeft)(times(1))
    planner.called(`(c) ((x)-[r]->(y))+ (d)`, fromRight)(times(0))
  }

  test("Should cache 1 plan per identical request") {
    val planner = mockPlanner()
    val cache = new CacheBackedQPPInnerPlanner(planner)

    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromRight)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromRight)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft)
    cache.planQPP(`(c) ((x)-[r]->(y))+ (d)`, fromLeft)

    cache.entries(`(a) ((n)-[r]->(m))+ (b)`, fromLeft) should contain theSameElementsAs Map(
      CacheKeyInner(Set.empty) -> dummyPlan
    )
    cache.entries(`(a) ((n)-[r]->(m))+ (b)`, fromRight) should contain theSameElementsAs Map(
      CacheKeyInner(Set.empty) -> dummyPlan
    )
    cache.entries(`(c) ((x)-[r]->(y))+ (d)`, fromLeft) should contain theSameElementsAs Map(
      CacheKeyInner(Set.empty) -> dummyPlan
    )
    cache.isEmpty(`(c) ((x)-[r]->(y))+ (d)`, fromRight) shouldBe true
  }

  test("Should not cache more plan than its maximum capacity allows") {
    val planner = mockPlanner()
    val cache = new CacheBackedQPPInnerPlanner(planner)

    Range(0, 10000).foreach(n => {
      val cacheBuster = ExtractedPredicates(
        requiredSymbols = Set(UnPositionedVariable.varFor(n.toString)),
        predicates = Seq.empty
      )
      cache.planQPP(`(a) ((n)-[r]->(m))+ (b)`, fromLeft, cacheBuster)
    })

    cache.entries(`(a) ((n)-[r]->(m))+ (b)`, fromLeft).size shouldBe cache.CACHE_MAX_SIZE
  }
}

object CacheBackedQPPInnerPlannerTest extends CypherFunSuite {

  implicit class QPPInnerPlannerOps(planner: QPPInnerPlanner) {

    def called(qpp: QuantifiedPathPattern, fromLeft: Boolean)(mode: VerificationMode): LogicalPlan =
      verify(planner, mode).planQPP(
        qpp,
        fromLeft,
        ExtractedPredicates(Set.empty, Seq.empty)
      )
  }

  implicit class QPPInnerPlansOps(cache: CacheBackedQPPInnerPlanner) {

    def entries(
      qpp: QuantifiedPathPattern,
      fromLeft: Boolean
    ): Map[CacheBackedQPPInnerPlanner.CacheKeyInner, LogicalPlan] = {
      val cacheKeyTop = CacheKeyOuter(qpp, fromLeft)
      Map.from(MapHasAsScala(cache.caches(cacheKeyTop).asMap()).asScala)
    }

    def isEmpty(qpp: QuantifiedPathPattern, fromLeft: Boolean): Boolean = {
      val cacheKeyTop = CacheKeyOuter(qpp, fromLeft)
      !cache.caches.contains(cacheKeyTop)
    }
  }

  def mockPlanner(): QPPInnerPlanner = {
    val planner = mock[QPPInnerPlanner]
    when(planner.planQPP(any, any, any)).thenReturn(dummyPlan)
    planner
  }

  val dummyPlan: LogicalPlan = Argument(Set.empty)(SameId(Id(0)))

  val fromLeft: Boolean = true

  val fromRight: Boolean = false

  val `(a) ((n)-[r]->(m))+ (b)` : QuantifiedPathPattern = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"n", v"a"),
    rightBinding = NodeBinding(v"m", v"b"),
    patternRelationships =
      NonEmptyList(PatternRelationship(v"r", (v"n", v"m"), OUTGOING, Seq.empty, SimplePatternLength)),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.unlimited),
    nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
    relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
  )

  val `(c) ((x)-[r]->(y))+ (d)` : QuantifiedPathPattern = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"x", v"c"),
    rightBinding = NodeBinding(v"y", v"d"),
    patternRelationships =
      NonEmptyList(PatternRelationship(v"r", (v"x", v"y"), OUTGOING, Seq.empty, SimplePatternLength)),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.unlimited),
    nodeVariableGroupings = Set(VariableGrouping(v"x", v"x"), VariableGrouping(v"y", v"y")),
    relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
  )
}
