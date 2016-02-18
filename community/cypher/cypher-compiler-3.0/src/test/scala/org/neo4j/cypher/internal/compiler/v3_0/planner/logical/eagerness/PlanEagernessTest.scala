/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{Cardinality, LogicalPlanningContext, LogicalPlanningFunction3}
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class PlanEagernessTest extends CypherFunSuite with LogicalPlanConstructionTestSupport with AstConstructionTestSupport with PlannerQueryTestSupport {

  private implicit var context: LogicalPlanningContext = null
  private var lpp: LogicalPlanProducer = null

  // Logical Plans
  private val allNodesScan = (name: String) => AllNodesScan(IdName(name), Set.empty)(CardinalityEstimation.lift(RegularPlannerQuery(QueryGraph(patternNodes = Set(IdName(name)))), Cardinality.SINGLE))
  private val argument = Argument(Set('x))(solved)(Map.empty)
  private val apply = Apply(argument, allNodesScan("a"))(solved)
  private val singleRow = SingleRow()(solved)
  private val innerUpdatePlanner: FakePlanner = spy(FakePlanner())
  private val eagernessPlanner: PlanEagerness = PlanEagerness(innerUpdatePlanner)

  override protected def initTest(): Unit = {
    super.initTest()
    context = mock[LogicalPlanningContext]
    lpp = mock[LogicalPlanProducer]
    when(context.logicalPlanProducer).thenReturn(lpp)
    when(lpp.planEager(any())).thenAnswer(new Answer[LogicalPlan] {
      override def answer(invocation: InvocationOnMock): LogicalPlan =
        eager(invocation.getArguments.apply(0).asInstanceOf[LogicalPlan])
    })
    reset(innerUpdatePlanner)
  }

  test("MATCH only") {
    val lhs = allNodesScan("a")
    val pq = RegularPlannerQuery(argumentQG("a"))
    val result = eagernessPlanner.apply(pq, lhs, head = false)

    result should equal(updateX(lhs))
  }

  test("overlapping MATCH and CREATE in a tail") {
    // given
    val lhs = allNodesScan("x")
    val pq = RegularPlannerQuery(createNodeQG("b") ++ matchNode("a"))

    // when
    val result = eagernessPlanner.apply(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("overlapping MATCH and CREATE in head") {
    val lhs = allNodesScan("a")
    val pq = RegularPlannerQuery(createNodeQG("b") ++ matchNode("a"))
    val result = eagernessPlanner.apply(pq, lhs, head = true)

    result should equal(updateX(lhs))
  }

  test("overlapping MATCH with two nodes and CREATE in head") {
    // given
    val lhs = CartesianProduct(allNodesScan("a"), allNodesScan("b"))(null)
    val pq = RegularPlannerQuery(matchNode("a") ++ matchNode("b") ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner.apply(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("do not consider stable node ids when checking overlapping MATCH and CREATE in head") {
    // given
    val lhs = apply
    val pq = RegularPlannerQuery(argumentQG("a") ++ matchNode("b") ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("single node MERGE") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("MERGE followed by CREATE on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(updateX(lhs)))
  }

  test("CREATE followed by MERGE on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(updateX(lhs)))
  }

  test("MERGE followed by CREATE on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("CREATE followed by MERGE on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("MERGE followed by MATCH on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(updateX(lhs)))
  }

  test("MATCH followed by MERGE on overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(updateX(lhs)))
  }

  test("MERGE followed by MATCH on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("MATCH followed by MERGE on not overlapping nodes") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(mergeNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("CREATE in head followed by MATCH") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "a"))
    val tail = RegularPlannerQuery(matchNode(name = "b"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("CREATE followed by MATCH on overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(updateX(lhs)))
  }

  test("MATCH followed by CREATE on overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "A"))
    val tail = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(eager(updateX(lhs)))
  }

  test("CREATE followed by MATCH on not overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))
    val tail = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("MATCH followed by CREATE on not overlapping nodes with labels") {
    // given
    val lhs = singleRow
    val pq = RegularPlannerQuery(matchNodeQG(name = "b", label = "B"))
    val tail = RegularPlannerQuery(createNodeQG(name = "a", label = "A"))

    // when
    val result = eagernessPlanner(pq.withTail(tail), lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("MATCH with labels followed by CREATE without") {
    // given
    val lhs = singleRow
    val readQG = matchNodeQG("a", "A")
    val qg = readQG ++ createNodeQG("b")
    val pq = RegularPlannerQuery(qg ++ createNodeQG("b"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("MATCH with labels followed by SET label") {
    // given
    val lhs = singleRow
    val readQG = matchNodeQG("a", "A")
    val pq = RegularPlannerQuery(readQG withMutation setLabel("a", "A"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("two MATCHes with labels followed by SET label") {
    // given
    val lhs = singleRow
    val qg = matchNodeQG("a", "A") ++ matchNode("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("MATCH with property followed by SET property") {
    // given
    val lhs = singleRow
    val readQG = matchNode("a") withPredicate propEquality("a", "prop", 42)
    val pq = RegularPlannerQuery(readQG withMutation setNodeProperty("a", "prop"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("two MATCHes with property followed by SET property") {
    // given
    val lhs = singleRow
    val readQG = (matchNode("a") withPredicate propEquality("a", "prop", 42)) ++ matchNode("b")
    val pq = RegularPlannerQuery(readQG withMutation setNodeProperty("b", "prop"))

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("Protect MATCH from REMOVE label") {
    // given

    val lhs = singleRow
    val firstQG = matchNodeQG("a", "A") ++ matchNode("b") withMutation removeLabel("b", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("When removing label from variable found through said label, no eagerness is needed") {
    // given

    val lhs = singleRow
    val firstQG = matchNodeQG("a", "A") withMutation removeLabel("a", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("Matching on one label and removing another is OK") {
    // given

    val lhs = singleRow
    val firstQG = matchNode("a") ++
                  matchNodeQG("b", "B") withMutation removeLabel("b", "B")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("MATCH (n) CREATE (m) WITH * MATCH (o {prop:42}) SET n.prop2 = 42") {
    // given

    val lhs = allNodesScan("n")
    val firstQG = matchNode("n") withMutation createNode("m")
    val secondQG = matchNode("o") withPredicate propEquality("o","prop",42)  withMutation setNodeProperty("n", "prop2")
    val pq = RegularPlannerQuery(firstQG) withTail RegularPlannerQuery(secondQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("MATCH (n) CREATE (m) in tail is not safe") {
    // given
    val lhs = allNodesScan("n")
    val firstQG = matchNode("n") withMutation createNode("m")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("Protect MATCH from SET label") {
    // given

    val lhs = singleRow
    val firstQG = matchNodeQG("a", "A") ++ matchNode("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("Protect OPTIONAL MATCH from SET label") {
    // given

    val lhs = singleRow

    val firstQG =  QueryGraph.empty.withAddedOptionalMatch(matchNodeQG("a", "A")) ++ matchNode("b") withMutation setLabel("b", "A")
    val pq = RegularPlannerQuery(firstQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("match relationship without any updates") {
    // given
    val lhs = singleRow
    val qg = MATCH('a -> ('r :: 'T) -> 'c)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("match relationship and then create relationship of same type") {
    // given
    val lhs = singleRow
    val qg = MATCH('a  -> ('r :: 'T) -> 'b) withMutation createRel('a -> ('r2 :: 'T) -> 'b)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("match relationship and then create relationship of same type, but with different properties") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation (createRel('a -> ('r2 :: 'T) -> 'b) andSetProperty("prop2", 42))
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("match relationship and then create relationship of different type needs no eager") {
    // given
    val lhs = singleRow
    val qg = MATCH('a  -> ('r :: 'T) -> 'b) withMutation createRel('a -> ('r2 :: 'T2) -> 'b)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("match relationship and set properties on node from unknown map") {
    // given MATCH (a)-[r:T]->(b {prop: 42}) SET a = {param}
    val lhs = singleRow
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("b", "prop", 42)
    val qg = readPart withMutation SetNodePropertiesFromMapPattern("a", Parameter("param")(pos), removeOtherProps = false)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("match relationship and set properties on node from known map") {
    // given MATCH (a)-[r:T]->(b {prop: 42}) SET a = {other: 42}
    val lhs = singleRow
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("b", "prop", 42)

    val mapExpression = MapExpression(Seq((PropertyKeyName("other")(pos), literalInt(42))))(pos)
    val qg = readPart withMutation SetNodePropertiesFromMapPattern("a", mapExpression, removeOtherProps = false)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = false)

    // then
    result should equal(updateX(lhs))
  }

  test("the stable left-most leaf does not solve all predicates needed with map-expression") {
    // given MATCH (a {prop: 42})-[r:T]->(b) SET b += { prop: 42 }
    val predicate = propEquality("a", "prop", 42)
    val lhs = Selection(Seq(predicate), allNodesScan("a"))(solved)
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate predicate

    val mapExpression = MapExpression(Seq((PropertyKeyName("prop")(pos), literalInt(42))))(pos)
    val qg = readPart withMutation SetNodePropertiesFromMapPattern("b", mapExpression, removeOtherProps = false)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("the stable left-most leaf does not solve all predicates needed") {
    // given MATCH (a {prop: 42})-[r:T]->(b) SET b.prop += 42
    val predicate = propEquality("a", "prop", 42)
    val lhs = Selection(Seq(predicate), allNodesScan("a"))(solved)
    val readPart = MATCH('a -> ('r :: 'T) -> 'b) withPredicate predicate

    val qg = readPart withMutation setNodeProperty("b", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("MATCH (a {prop : 5})-[r]-(b) SET b.prop2 = 5") {
    val lhs = singleRow
    val readPart = MATCH('a -- ('r :: 'T) -- 'b) withPredicate propEquality("a", "prop", 5)
    val qg = readPart withMutation setNodeProperty("b", "prop2")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("Should not be eager when matching relationship and not writing to rels") {
    val lhs = singleRow
    val qg = MATCH('a -- 'r -- 'b) withMutation setNodeProperty("a", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("Read relationship and update it in a way that does not fit the predicate on the relationship") {
    val lhs = singleRow
    val qg = MATCH('a -> 'r -> 'b) withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("read two relationships, one with filter, set prop on other that matches filter") {
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b) ++ MATCH('a2 -> 'r2 -> 'b2) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r2", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("read two directed relationships, one with filter, set prop on same that matches filter") {
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b) ++ MATCH('a2 -> 'r2 -> 'b2) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("when undirected, matching and setting needs eagerness") {
    val lhs = singleRow
    val read = MATCH('a -- 'r -- 'b) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("when directed, matching and setting does not need eagerness") {
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b) withPredicate propEquality("r", "prop", 42)
    val qg = read withMutation setRelProperty("r", "prop")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("when directed, matching and deleting does not need eagerness") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'b)
    val qg = read withMutation delete("r")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("when undirected, matching and deleting needs eagerness") {
    // given
    val lhs = singleRow
    val read = MATCH('a -- 'r -- 'b)
    val qg = read withMutation delete("a") withMutation delete("r") withMutation delete("b")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("undirected relationship, and we are creating new matching relationships with matching type and props") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> ('r :: 'T) -> 'b) withPredicate propEquality("r", "prop", 43)
    val qg = read withMutation (createRel('a -> ('r2 :: 'T) ->'b) andSetProperty ("prop", 43))
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("match with unstable leaf and then delete needs eagerness") {
    // given
    val lhs = singleRow
    val read = matchNodeQG("a", "A") ++ matchNodeQG("b", "B")
    val qg = read withMutation delete("a") withMutation delete("b")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("match relationship and delete path") {
    // given
    val lhs = singleRow
    val read = MATCH('a -> 'r -> 'r)
    val qg = read withMutation delete('a -> 'r -> 'r)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("match, delete and then merge a node") {
    // given
    val lhs = singleRow
    val firstQ = matchNodeQG("b", "B") withMutation delete("b")
    val secondQG = mergeNodeQG("c", "B", "prop", 42)
    val pq = RegularPlannerQuery(firstQ) withTail RegularPlannerQuery(secondQG)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(eager(updateX(eager(lhs))))
  }

  test("match relationship and then create single node") {
    // given
    val lhs = singleRow
    val qg = MATCH('a -> 'r -> 'b) withMutation createNode("c")
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

  test("cartesian product and then create one node that matches both") {
    // given
    val lhs = singleRow
    val readQg = matchNode("a") ++ matchNode("b") withPredicate
      propEquality("a", "prop1", 42) withPredicate
      propEquality("b", "prop2", 42)
    val qg = readQg withMutation (createNode("c") andSetProperty("prop1", 42) andSetProperty("prop2", 42))

    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(eager(lhs)))
  }

  test("when eager is needed to protect a read against a future write, apply eagerness after the updates instead of before") {
    // given
    // MATCH (b {prop: 42}) -- QG1
    // CREATE (c)
    // WITH *
    // MATCH (d) -- QG2
    // SET c.prop = 42
    // RETURN count(*) as count
    val lhs = singleRow
    val qg1 = matchNode("b") withPredicate propEquality("b", "prop", 42) withMutation createNode("c")
    val qg2 = matchNode("d") withMutation setNodeProperty("c", "prop")

    val pq = RegularPlannerQuery(qg1) withTail RegularPlannerQuery(qg2)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(eager(updateX(lhs)))
  }

  ignore("match single directed relationship and delete it does not need eagerness") {
    /*given
    MATCH (a)-[t]->(b)
    DELETE t
    MERGE (a)-[t2:T2]->(b)
    RETURN exists(t2.id)
    */
    val lhs = singleRow
    val qg = MATCH('a -> 'r -> 'b) withMutation delete("r")
    val mergeQG = merge('a -> 'r -> 'b)
    val pq = RegularPlannerQuery(qg)

    // when
    val result = eagernessPlanner(pq, lhs, head = true)

    // then
    result should equal(updateX(lhs))
  }

//  test("")

}

case class updateX(inner: LogicalPlan) extends LogicalPlan {
  override def lhs = Some(inner)
  override def rhs = None

  override def solved: PlannerQuery with CardinalityEstimation = ???

  override def availableSymbols: Set[IdName] = ???

  override def mapExpressions(f: (Set[IdName], Expression) => Expression): LogicalPlan = ???

  override def strictness: StrictnessMode = ???
}

case class FakePlanner() extends LogicalPlanningFunction3[PlannerQuery, LogicalPlan, Boolean, LogicalPlan] {
  override def apply(plannerQuery: PlannerQuery, lhs: LogicalPlan, head: Boolean)
                    (implicit context: LogicalPlanningContext): LogicalPlan = updateX(lhs)
}

