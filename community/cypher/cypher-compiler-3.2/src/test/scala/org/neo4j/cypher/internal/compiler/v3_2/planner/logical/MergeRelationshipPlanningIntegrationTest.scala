/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.pipes.LazyType
import org.neo4j.cypher.internal.compiler.v3_2.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.InputPosition
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.IdName

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = IdName("a")
  private val bId = IdName("b")
  private val rId = IdName("r")
  private val argId = IdName("arg")

  private def isNull(x: IdName) = IsNull(varFor(x.name))(InputPosition.NONE)

  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set.empty)(solved)
    val expand = Expand(nodeByLabelScan, aId, OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand)(solved)
    val argument = SingleRow()(solved)
    val createNodeA = MergeCreateNode(argument, aId, Seq(LabelName("A")(pos)), None)(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)
    val predicate = Ors(Set(isNull(aId), isNull(bId), isNull(rId)))(InputPosition.NONE)
    val mergeNode = ConditionalApply(optional, onCreate, predicate)(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a:A)-[r:R]->(b)")._2 should equal(emptyResult)
  }

  test("should plan simple expand with argument dependency") {

    // MERGE Create side
    val argBeforeMerge = Argument(Set(argId))(solved)(Map.empty)
    val labelName = LabelName("A")(pos)
    val propertyKeyName = PropertyKeyName("p")(pos)
    val createNodeA = MergeCreateNode(argBeforeMerge, aId, Seq(labelName), Some(MapExpression(Seq((propertyKeyName, varFor("arg"))))(pos)))(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)
    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)

    // MERGE Optional match
    val nodeByLabelScan = NodeByLabelScan(aId, labelName, Set(argId))(solved)
    val selection = Selection(Seq(In(Property(Variable("a")(pos), propertyKeyName)(pos), ListLiteral(Seq(varFor("arg")))(pos))(pos)), nodeByLabelScan)(solved)
    val expand = Expand(selection, aId, OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)
    val optional = Optional(expand, Set(argId))(solved)
    val predicate = Ors(Set(isNull(aId), isNull(bId), isNull(rId)))(InputPosition.NONE)
    val mergeLockS = MergeLock(argBeforeMerge, Seq(LockDescription(labelName, Seq(propertyKeyName -> varFor("arg")))), Shared)(solved)
    val matchWithSLock = Apply(mergeLockS, optional)(solved)
    val argInsideMerge = Argument(Set(argId, aId, bId, rId))(solved)(Map.empty)
    val mergeLockX = MergeLock(argInsideMerge, Seq(LockDescription(labelName, Seq(propertyKeyName -> varFor("arg")))), Exclusive)(solved)
    val matchWithXLock = Apply(mergeLockX, optional)(solved)
    val lockedMatch = ConditionalApply(matchWithSLock, matchWithXLock, predicate)(solved)

    val mergeNode = ConditionalApply(lockedMatch, onCreate, predicate)(solved)

    // source including the `arg` variable
    val leaf = SingleRow()(solved)
    val projection = Projection(leaf, Map("arg" -> SignedDecimalIntegerLiteral("42")(pos)))(solved)

    val apply = Apply(projection, mergeNode)(solved)
    val emptyResult = EmptyResult(apply)(solved)

    planFor("WITH 42 AS arg MERGE (a:A {p: arg})-[r:R]->(b)")._2 should equal(emptyResult)
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {

    val predicate = Ors(Set(isNull(bId), isNull(rId)))(InputPosition.NONE)


    planFor("MATCH (n) MERGE (n)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          AllNodesScan(IdName("n"), Set())(solved),
          ConditionalApply(
            ConditionalApply(
              Optional(
                Expand(
                  Argument(Set(IdName("n")))(solved)(),
                  IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("n")))(solved),
              Optional(
                Expand(
                  LockNodes(Argument(Set(IdName("n")))(solved)(), Set(IdName("n")))(solved),
                  IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("n")))(solved),
              predicate)(solved),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set(IdName("n")))(solved)(),
                IdName("b"), Seq.empty, None)(solved),
              IdName("r"), IdName("n"), LazyType("T"), IdName("b"), None)(solved),
            predicate)(solved)
        )(solved)
      )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val plan = planFor("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)")._2
    val predicate = isNull(rId)
    plan should equal(EmptyResult(
      Apply(
        CartesianProduct(
          AllNodesScan(IdName("n"), Set())(solved),
          AllNodesScan(IdName("m"), Set())(solved)
        )(solved),
        ConditionalApply(
          ConditionalApply(
            Optional(
              Expand(
                Argument(Set(IdName("n"), IdName("m")))(solved)(),
                IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("m"), IdName("r"), ExpandInto)(solved),
              Set(IdName("n"), IdName("m")))(solved),
            Optional(
              Expand(
                LockNodes(
                  Argument(Set(IdName("n"), IdName("m")))(solved)(),
                  Set(IdName("n"), IdName("m")))(solved),
                IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("m"), IdName("r"), ExpandInto)(solved),
              Set(IdName("n"), IdName("m")))(solved),
            predicate)(solved),
          MergeCreateRelationship(
            Argument(Set(IdName("n"), IdName("m")))(solved)(),
            IdName("r"), IdName("n"), LazyType("T"), IdName("m"), None)(solved),
          predicate)(solved)
      )(solved)
    )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    val predicate = isNull(rId)

    planFor("MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            CartesianProduct(
              AllNodesScan(IdName("n"), Set())(solved),
              AllNodesScan(IdName("m"), Set())(solved)
            )(solved),
            Map("a" -> Variable("n")(pos), "b" -> Variable("m")(pos))
          )(solved),
          ConditionalApply(
            ConditionalApply(
              Optional(
                Expand(
                  Argument(Set(IdName("a"), IdName("b")))(solved)(),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandInto)(solved),
                Set(IdName("a"), IdName("b"))
              )(solved),
              Optional(
                Expand(
                  LockNodes(
                    Argument(Set(IdName("a"), IdName("b")))(solved)(), Set(IdName("a"), IdName("b")))(solved),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandInto)(solved),
                Set(IdName("a"), IdName("b"))
              )(solved),
              predicate)(solved),
            MergeCreateRelationship(
              Argument(Set(IdName("a"), IdName("b")))(solved)(),
              IdName("r"), IdName("a"), LazyType("T"), IdName("b"), None)(solved),
            predicate)(solved)
        )(solved)
      )(solved)
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    val predicate = Ors(Set(isNull(bId), isNull(rId)))(InputPosition.NONE)

    planFor("MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            AllNodesScan(IdName("n"), Set())(solved),
            Map("a" -> Variable("n")(pos))
          )(solved),
          ConditionalApply(
            ConditionalApply(
              Optional(
                Expand(
                  Argument(Set(IdName("a")))(solved)(),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("a"))
              )(solved),
              Optional(
                Expand(
                  LockNodes(Argument(Set(IdName("a")))(solved)(), Set(IdName("a")))(solved),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("a"))
              )(solved),
              predicate)(solved),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set(IdName("a")))(solved)(),
                IdName("b"), Seq.empty, None)(solved),
              IdName("r"), IdName("a"), LazyType("T"), IdName("b"), None)(solved),
            predicate)(solved)
        )(solved)
      )(solved)
    )
  }
}
