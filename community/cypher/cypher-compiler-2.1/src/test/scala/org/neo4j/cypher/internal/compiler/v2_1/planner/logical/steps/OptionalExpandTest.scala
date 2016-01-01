/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Candidates, PlanTable, CandidateList}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._


class OptionalExpandTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  val patternRel1 = PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val patternRel2 = PatternRelationship("r2", ("c", "a"), Direction.INCOMING, Seq.empty, SimplePatternLength)
  val patternRel3 = PatternRelationship("r3", ("b", "a"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val patternRel4 = PatternRelationship("r4", ("b", "x"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
  val r1Predicate: Equals = Equals(Property(Identifier("r1")_, PropertyKeyName("prop")_)_, StringLiteral("foo")_)_
  val bPredicate: Equals = Equals(Property(Identifier("b")_, PropertyKeyName("prop")_)_, StringLiteral("foo")_)_
  val aAndR1Predicate: Equals = Equals(Property(Identifier("a")_, PropertyKeyName("prop")_)_, Property(Identifier("r1")_, PropertyKeyName("prop")_)_)_

  test("should introduce optional expand for unsolved optional match when all arguments are covered and there's a single unsolved pattern relationship") {
    // MATCH (a) OPTIONAL MATCH (a)-[r]->(b)
    val optionalMatch = QueryGraph(patternNodes = Set("a", "b"), patternRelationships = Set(patternRel1))
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val inputPlan = planArgumentRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = planOptionalExpand(inputPlan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), SimplePatternLength, Seq.empty, optionalMatch)

    optionalExpand(planTable, qg) should equal(Candidates(innerPlan))
  }

  test("should not introduce optional expand when there's more than one unsolved pattern relationship") {
    // MATCH (a) OPTIONAL MATCH (c)<-[r2]-(a)-[r1]->(b)
    val optionalMatch = QueryGraph(
      patternNodes = Set("a", "b", "c"),
      patternRelationships = Set(patternRel1, patternRel2))

    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val inputPlan = planArgumentRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    optionalExpand(planTable, qg) should equal(Candidates())
  }

  test("should introduce optional expand and bring along predicates on the relationship") {
    // MATCH (a) OPTIONAL MATCH (a)-[r1]->(b) WHERE r1.foo = 42
    val optionalMatch = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel1),
      selections = Selections(Set(Predicate(Set(IdName("r1")), r1Predicate))))
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val inputPlan = planArgumentRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = planOptionalExpand(inputPlan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), SimplePatternLength, Seq(r1Predicate), optionalMatch)

    optionalExpand(planTable, qg) should equal(Candidates(innerPlan))
  }

  test("should introduce optional expand and bring along predicates on the end node") {
    // MATCH (a) OPTIONAL MATCH (a)-[r1]->(b) WHERE b.foo = 42
    val optionalMatch = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel1),
      selections = Selections(Set(Predicate(Set(IdName("b")), bPredicate))))
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val inputPlan = planArgumentRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = planOptionalExpand(inputPlan, IdName("a"), Direction.OUTGOING, Seq.empty, IdName("b"), IdName("r1"), SimplePatternLength, Seq(bPredicate), optionalMatch)

    optionalExpand(planTable, qg) should equal(Candidates(innerPlan))
  }

  test("should introduce optional expand with the right relationship direction") {
    // MATCH (a) OPTIONAL MATCH (b)-[r1]->(a) WHERE b.foo = 42
    val optionalMatch = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel3),
      selections = Selections(Set(Predicate(Set(IdName("b")), bPredicate))))
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val inputPlan = planArgumentRow(Set("a"))
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = planOptionalExpand(inputPlan, IdName("a"), Direction.INCOMING, Seq.empty, IdName("b"), IdName("r3"), SimplePatternLength, Seq(bPredicate), optionalMatch)

    optionalExpand(planTable, qg) should equal(Candidates(innerPlan))
  }

  test("should not introduce optional expand until predicates have their dependencies satisfied") {
    // MATCH (a)-->(b) OPTIONAL MATCH (b)-[r1]->(x) WHERE r1.foo = a.foo
    val optionalMatch = QueryGraph(
      patternNodes = Set("b", "x"),
      patternRelationships = Set(patternRel4),
      selections = Selections(Set(Predicate(Set(IdName("r1"), IdName("a")), aAndR1Predicate))))
    val qg = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel1)).
      withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    val inputPlan = planArgumentRow(Set("b"))
    val planTable = PlanTable(Map(Set(IdName("b")) -> inputPlan))

    optionalExpand(planTable, qg) should equal(CandidateList(Seq()))
  }
}
