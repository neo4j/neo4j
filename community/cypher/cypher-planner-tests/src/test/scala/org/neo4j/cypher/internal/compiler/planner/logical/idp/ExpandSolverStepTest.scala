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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.DifferentNodes
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.DisjointNodes
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsRepeatAcyclic
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.NoneOfNodes
import org.neo4j.cypher.internal.expressions.NoneOfNodesInVarLengthRelationship
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.UniqueNodes
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode

import scala.collection.immutable.BitSet
import scala.language.implicitConversions

class ExpandSolverStepTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport2 {

  implicit def converter(s: Symbol): String = s.toString()

  private val pattern1 =
    PatternRelationship(v"r1", (v"a", v"b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private val pattern2 =
    PatternRelationship(v"r2", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

  private val table = IDPTable.empty[LogicalPlan]
  private val qg = QueryGraph.empty

  private val noQPPInnerPlans = new CacheBackedQPPInnerPlanner(???)

  test("does not expand based on empty table") {
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      expandSolverStep(qg, noQPPInnerPlans)(registry, register(pattern1, pattern2), table, ctx) should be(empty)
    }
  }

  test("expands if an unsolved pattern relationship overlaps once with a single solved plan") {
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      table.put(register(pattern1), sorted = false, hasExtraProperties = false, plan1)

      expandSolverStep(qg, noQPPInnerPlans)(registry, register(pattern1, pattern2), table, ctx).toSet should equal(Set(
        Expand(plan1, v"b", SemanticDirection.OUTGOING, Seq.empty, v"c", v"r2", ExpandAll)
      ))
    }
  }

  test("expands if an unsolved pattern relationships overlaps twice with a single solved plan") {
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      table.put(register(pattern1), sorted = false, hasExtraProperties = false, plan1) // a - [r1] - b

      val patternX =
        PatternRelationship(
          v"r2",
          (v"a", v"b"),
          SemanticDirection.OUTGOING,
          Seq.empty,
          SimplePatternLength
        ) // a - [r2] -> b

      expandSolverStep(qg, noQPPInnerPlans)(registry, register(pattern1, patternX), table, ctx).toSet should equal(Set(
        Expand(plan1, v"a", SemanticDirection.OUTGOING, Seq.empty, v"b", v"r2", ExpandInto),
        Expand(plan1, v"b", SemanticDirection.INCOMING, Seq.empty, v"a", v"r2", ExpandInto)
      ))
    }
  }

  test("does not expand if an unsolved pattern relationship does not overlap with a solved plan") {
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]
    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )
      table.put(register(pattern1), sorted = false, hasExtraProperties = false, plan1)

      val patternX =
        PatternRelationship(v"r2", (v"x", v"y"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

      expandSolverStep(qg, noQPPInnerPlans)(registry, register(pattern1, patternX), table, ctx).toSet should be(empty)
    }

  }

  test("expands if an unsolved pattern relationship overlaps with multiple solved plans") {
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b", "c", "r2", "d")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b", v"c", v"d"))
      )
      table.put(register(pattern1, pattern2), sorted = false, hasExtraProperties = false, plan1)

      val pattern3 =
        PatternRelationship(v"r3", (v"b", v"c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength)

      expandSolverStep(qg, noQPPInnerPlans)(
        registry,
        register(pattern1, pattern2, pattern3),
        table,
        ctx
      ).toSet should equal(Set(
        Expand(plan1, v"b", SemanticDirection.OUTGOING, Seq.empty, v"c", v"r3", ExpandInto),
        Expand(plan1, v"c", SemanticDirection.INCOMING, Seq.empty, v"b", v"r3", ExpandInto)
      ))
    }
  }

  test("does not expand if goal is entirely compacted") {
    implicit val registry: DefaultIdRegistry[NodeConnection] = IdRegistry[NodeConnection]

    new givenConfig().withLogicalPlanningContext { (_, ctx) =>
      val plan1 = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "a", "r1", "b")
      ctx.staticComponents.planningAttributes.solveds.set(
        plan1.id,
        RegularSinglePlannerQuery(QueryGraph.empty.addPatternNodes(v"a", v"b"))
      )

      val compactedPattern1 = Goal(BitSet(registry.compact(register(pattern1).bitSet)))
      val compactedPattern2 = Goal(BitSet(registry.compact(register(pattern2).bitSet)))

      table.put(compactedPattern1, sorted = false, hasExtraProperties = false, plan1)

      expandSolverStep(qg, noQPPInnerPlans)(
        registry,
        Goal(compactedPattern1.bitSet ++ compactedPattern2.bitSet),
        table,
        ctx
      ).toSet should be(empty)
    }
  }

  def register(patRels: NodeConnection*)(implicit registry: IdRegistry[NodeConnection]): Goal =
    Goal(registry.registerAll(patRels))

  private val nodeUniquenessPredicates: Seq[Expression] = Seq(
    DifferentNodes(v"n1", v"n2")(pos),
    NoneOfNodes(v"n1", v"group")(pos),
    NoneOfNodesInVarLengthRelationship(v"n1", v"r1", SemanticDirection.OUTGOING, mustBeInlined = false)(pos),
    DisjointNodes(v"n1", v"n2", 1L, Some(2L))(pos),
    UniqueNodes(v"group", None)(pos),
    IsRepeatAcyclic(v"n1")(pos)
  )

  private val relationshipUniquenessPredicates: Seq[Expression] = Seq(
    DifferentRelationships(v"r1", v"r2")(pos),
    NoneOfRelationships(v"r1", v"group")(pos),
    Disjoint(v"r1", v"r2")(pos),
    Unique(v"group")(pos),
    IsRepeatTrailUnique(v"r1")(pos)
  )

  private val nonUniquenessPredicate: Expression = Equals(v"n1", v"n2")(pos)

  private def forAllRepetitions(inner: Expression): Expression =
    ForAllRepetitions(v"group", Set(VariableGrouping(v"n1", v"group")(pos)), inner)(pos)

  private val allPathModes = Seq(TraversalPathMode.Walk, TraversalPathMode.Trail, TraversalPathMode.Acyclic)

  for {
    predicate <- nodeUniquenessPredicates
    (wrapperName, wrap) <-
      Seq[(String, Expression => Expression)](("", identity), ("ForAllRepetitions-wrapped ", forAllRepetitions))
  } {
    test(s"${wrapperName}node-uniqueness predicate ${predicate.getClass.getSimpleName} is enforced by Acyclic only") {
      expandSolverStep.enforcedByPathMode(wrap(predicate), TraversalPathMode.Acyclic) shouldBe true
      expandSolverStep.enforcedByPathMode(wrap(predicate), TraversalPathMode.Trail) shouldBe false
      expandSolverStep.enforcedByPathMode(wrap(predicate), TraversalPathMode.Walk) shouldBe false
    }
  }

  for {
    predicate <- relationshipUniquenessPredicates
    (wrapperName, wrap) <-
      Seq[(String, Expression => Expression)](("", identity), ("ForAllRepetitions-wrapped ", forAllRepetitions))
  } {
    test(
      s"${wrapperName}relationship-uniqueness predicate ${predicate.getClass.getSimpleName} is enforced by Acyclic and Trail"
    ) {
      expandSolverStep.enforcedByPathMode(wrap(predicate), TraversalPathMode.Acyclic) shouldBe true
      expandSolverStep.enforcedByPathMode(wrap(predicate), TraversalPathMode.Trail) shouldBe true
      expandSolverStep.enforcedByPathMode(wrap(predicate), TraversalPathMode.Walk) shouldBe false
    }
  }

  test("a non-uniqueness expression is not enforced by any path mode") {
    for (pathMode <- allPathModes) {
      expandSolverStep.enforcedByPathMode(nonUniquenessPredicate, pathMode) shouldBe false
      expandSolverStep.enforcedByPathMode(forAllRepetitions(nonUniquenessPredicate), pathMode) shouldBe false
    }
  }
}
