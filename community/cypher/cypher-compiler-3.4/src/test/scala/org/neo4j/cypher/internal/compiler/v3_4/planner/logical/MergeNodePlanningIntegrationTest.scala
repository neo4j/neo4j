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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.v3_4.logical.plans._

class MergeNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = IdName("a")
  private val bId = IdName("b")

  test("should plan single merge node") {
    val allNodesScan = AllNodesScan(aId, Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val onCreate = MergeCreateNode(SingleRow()(solved), aId, Seq.empty, None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a)")._2 should equal(emptyResult)
  }

  test("should plan single merge node from a label scan") {

    val labelScan = NodeByLabelScan(aId, lblName("X"), Set.empty)(solved)
    val optional = Optional(labelScan)(solved)
    val onCreate = MergeCreateNode(SingleRow()(solved), aId, Seq(lblName("X")), None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    (new given {
      labelCardinality = Map(
        "X" -> 30.0
      )
    } getLogicalPlanFor "MERGE (a:X)")._2 should equal(emptyResult)
  }

  test("should plan single merge node with properties") {

    val allNodesScan = AllNodesScan(aId, Set.empty)(solved)
    val propertyKeyName = PropertyKeyName("prop")(pos)
    val propertyValue = SignedDecimalIntegerLiteral("42")(pos)
    val selection = Selection(Seq(In(Property(Variable("a")(pos), propertyKeyName)(pos),
                                     ListLiteral(Seq(propertyValue))(pos))(pos)), allNodesScan)(solved)
    val optional = Optional(selection)(solved)

    val onCreate = MergeCreateNode(SingleRow()(solved), aId, Seq.empty,
      Some(MapExpression(List((PropertyKeyName("prop")(pos),
        SignedDecimalIntegerLiteral("42")(pos))))(pos)))(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a {prop: 42})")._2 should equal(emptyResult)
  }

  test("should plan create followed by merge") {
    val createNode = CreateNode(SingleRow()(solved), aId, Seq.empty, None)(solved)
    val allNodesScan = AllNodesScan(bId, Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val onCreate = MergeCreateNode(SingleRow()(solved), bId, Seq.empty, None)(solved)
    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(bId))(solved)
    val apply = Apply(createNode, mergeNode)(solved)
    val emptyResult = EmptyResult(apply)(solved)

    planFor("CREATE (a) MERGE (b)")._2 should equal(emptyResult)
  }

  test("should plan merge followed by create") {
    val allNodesScan = AllNodesScan(aId, Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val onCreate = MergeCreateNode(SingleRow()(solved), aId, Seq.empty, None)(solved)
    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))(solved)
    val eager = Eager(mergeNode)(solved)
    val createNode = CreateNode(eager, bId, Seq.empty, None)(solved)
    val emptyResult = EmptyResult(createNode)(solved)

    planFor("MERGE(a) CREATE (b)")._2 should equal(emptyResult)
  }

  test("should use AssertSameNode when multiple unique index matches") {
    val plan = (new given {
      uniqueIndexOn("X", "prop")
      uniqueIndexOn("Y", "prop")
    } getLogicalPlanFor "MERGE (a:X:Y {prop: 42})")._2

    plan shouldBe using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should not use AssertSameNode when one unique index matches") {
    val plan = (new given {
      uniqueIndexOn("X", "prop")
    } getLogicalPlanFor "MERGE (a:X:Y {prop: 42})")._2

    plan should not be using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  /*
   *                     |
   *                antiCondApply
   *                  /    \
   *                 /  set property
   *                /       \
   *               /    merge create node
   *         condApply       \
   *            /    \       arg
   *       optional  set label
   *         /
   *    allnodes
   */
  test("should plan merge node with on create and on match ") {
    val allNodesScan = AllNodesScan(aId, Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val argument = Argument(Set(aId))(solved)(Map.empty)
    val setLabels = SetLabels(argument, aId, Seq(lblName("L")))(solved)
    val onMatch = ConditionalApply(optional, setLabels, Seq(aId))(solved)

    val singleRow = SingleRow()(solved)
    val mergeCreateNode = MergeCreateNode(singleRow, aId, Seq.empty, None)(solved)
    val createAndOnCreate = SetNodeProperty(mergeCreateNode, aId, PropertyKeyName("prop")(pos), SignedDecimalIntegerLiteral("1")(pos))(solved)
    val mergeNode = AntiConditionalApply(onMatch, createAndOnCreate, Seq(aId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a) ON CREATE SET a.prop = 1 ON MATCH SET a:L")._2 should equal(emptyResult)
  }
}
