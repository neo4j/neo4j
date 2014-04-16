package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.CandidateList
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.StringLiteral
import org.neo4j.cypher.internal.compiler.v2_1.planner.OptionalQueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.Selections
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.planner.MainQueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.PlanTable
import org.neo4j.cypher.internal.compiler.v2_1.ast.Property

class OptionalExpandTest extends CypherFunSuite with LogicalPlanningTestSupport {

  val patternRel1 = PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val patternRel2 = PatternRelationship("r2", ("c", "a"), Direction.INCOMING, Seq.empty, SimplePatternLength)
  val patternRel3 = PatternRelationship("r3", ("b", "a"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val patternRel4 = PatternRelationship("r4", ("b", "x"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val r1Predicate: Equals = Equals(Property(Identifier("r1")_, PropertyKeyName("prop")(None)_)_, StringLiteral("foo")_)_
  val bPredicate: Equals = Equals(Property(Identifier("b")_, PropertyKeyName("prop")(None)_)_, StringLiteral("foo")_)_
  val aAndR1Predicate: Equals = Equals(Property(Identifier("a")_, PropertyKeyName("prop")(None)_)_, Property(Identifier("r1")_, PropertyKeyName("prop")(None)_)_)_

  test("should introduce optional expand for unsolved optional match when all arguments are covered and there's a single unsolved pattern relationship") {
    // MATCH (a) OPTIONAL MATCH (a)-[r]->(b)

    val qg = MainQueryGraph(
      projections = Map.empty,
      selections = Selections(),
      patternNodes = Set("a"),
      patternRelationships = Set.empty,
      optionalMatches = Seq(OptionalQueryGraph(
        selections = Selections(),
        patternNodes = Set("a", "b"),
        patternRelationships = Set(patternRel1),
        argumentIds = Set("a")
      ))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val inputPlan = SingleRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = OptionalExpand(inputPlan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), SimplePatternLength, Seq.empty)(patternRel1)

    optionalExpand(planTable) should equal(CandidateList(Seq(innerPlan)))
  }

  test("should not introduce optional expand when there's more than one unsolved pattern relationship") {
    // MATCH (a) OPTIONAL MATCH (c)<-[r2]-(a)-[r1]->(b)

    val qg = MainQueryGraph(
      projections = Map.empty,
      selections = Selections(),
      patternNodes = Set("a"),
      patternRelationships = Set.empty,
      optionalMatches = Seq(OptionalQueryGraph(
        selections = Selections(),
        patternNodes = Set("a", "b", "c"),
        patternRelationships = Set(patternRel1, patternRel2),
        argumentIds = Set("a")
      ))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val inputPlan = SingleRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    optionalExpand(planTable) should equal(CandidateList(Seq.empty))
  }

  test("should introduce optional expand and bring along predicates on the relationship") {
    // MATCH (a) OPTIONAL MATCH (a)-[r1]->(b) WHERE r1.foo = 42

    val qg = MainQueryGraph(
      projections = Map.empty,
      selections = Selections(),
      patternNodes = Set("a"),
      patternRelationships = Set.empty,
      optionalMatches = Seq(OptionalQueryGraph(
        selections = Selections(Seq(Set(IdName("r1")) -> r1Predicate)),
        patternNodes = Set("a", "b"),
        patternRelationships = Set(patternRel1),
        argumentIds = Set("a")
      ))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val inputPlan = SingleRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = OptionalExpand(inputPlan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), SimplePatternLength, Seq(r1Predicate))(patternRel1)

    optionalExpand(planTable) should equal(CandidateList(Seq(innerPlan)))
  }

  test("should introduce optional expand and bring along predicates on the end node") {
    // MATCH (a) OPTIONAL MATCH (a)-[r1]->(b) WHERE b.foo = 42

    val qg = MainQueryGraph(
      projections = Map.empty,
      selections = Selections(),
      patternNodes = Set("a"),
      patternRelationships = Set.empty,
      optionalMatches = Seq(OptionalQueryGraph(
        selections = Selections(Seq(Set(IdName("b")) -> bPredicate)),
        patternNodes = Set("a", "b"),
        patternRelationships = Set(patternRel1),
        argumentIds = Set("a")
      ))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val inputPlan = SingleRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = OptionalExpand(inputPlan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), SimplePatternLength, Seq(bPredicate))(patternRel1)

    optionalExpand(planTable) should equal(CandidateList(Seq(innerPlan)))
  }

  test("should introduce optional expand with the right relationship direction") {
    // MATCH (a) OPTIONAL MATCH (b)-[r1]->(a) WHERE b.foo = 42

    val qg = MainQueryGraph(
      projections = Map.empty,
      selections = Selections(),
      patternNodes = Set("a"),
      patternRelationships = Set.empty,
      optionalMatches = Seq(OptionalQueryGraph(
        selections = Selections(Seq(Set(IdName("b")) -> bPredicate)),
        patternNodes = Set("a", "b"),
        patternRelationships = Set(patternRel3),
        argumentIds = Set("a")
      ))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val inputPlan = SingleRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = OptionalExpand(inputPlan, IdName("a"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r3"), SimplePatternLength, Seq(bPredicate))(patternRel3)

    optionalExpand(planTable) should equal(CandidateList(Seq(innerPlan)))
  }

  test("should not introduce optional expand until predicates have their dependencies satisfied") {
    // MATCH (a)-->(b) OPTIONAL MATCH (b)-[r1]->(x) WHERE r1.foo = a.foo

    val qg = MainQueryGraph(
      projections = Map.empty,
      selections = Selections(),
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel1),
      optionalMatches = Seq(OptionalQueryGraph(
        selections = Selections(Seq(Set(IdName("r1"), IdName("a")) -> aAndR1Predicate)),
        patternNodes = Set("b", "x"),
        patternRelationships = Set(patternRel4),
        argumentIds = Set(IdName("b"))
      ))
    )

    implicit val context = newMockedLogicalPlanContext(
      planContext = newMockedPlanContext,
      queryGraph = qg
    )

    val inputPlan = SingleRow(Set("b"))
    val planTable = PlanTable(Map(Set(IdName("b")) -> inputPlan))

    optionalExpand(planTable) should equal(CandidateList(Seq()))
  }
}
