/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

class MergeNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = "a"
  private val bId = "b"

  test("should plan single merge node") {
    val allNodesScan = AllNodesScan(aId, Set.empty)
    val optional = Optional(ActiveRead(allNodesScan))
    val onCreate = MergeCreateNode(Argument(), aId, Seq.empty, None)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))
    val emptyResult = EmptyResult(mergeNode)

    planFor("MERGE (a)")._2 should equal(emptyResult)
  }

  test("should plan single merge node from a label scan") {

    val labelScan = NodeByLabelScan(aId, lblName("X"), Set.empty)
    val optional = Optional(ActiveRead(labelScan))
    val onCreate = MergeCreateNode(Argument(), aId, Seq(lblName("X")), None)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))
    val emptyResult = EmptyResult(mergeNode)

    (new given {
      labelCardinality = Map(
        "X" -> 30.0
      )
    } getLogicalPlanFor "MERGE (a:X)")._2 should equal(emptyResult)
  }

  test("should plan single merge node with properties") {

    val allNodesScan = AllNodesScan(aId, Set.empty)
    val propertyKeyName = PropertyKeyName("prop")(pos)
    val propertyValue = SignedDecimalIntegerLiteral("42")(pos)
    val selection = Selection(Seq(In(Property(Variable("a")(pos), propertyKeyName)(pos),
                                     ListLiteral(Seq(propertyValue))(pos))(pos)), allNodesScan)
    val optional = Optional(ActiveRead(selection))

    val onCreate = MergeCreateNode(Argument(), aId, Seq.empty,
      Some(MapExpression(List((PropertyKeyName("prop")(pos),
        SignedDecimalIntegerLiteral("42")(pos))))(pos)))

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))
    val emptyResult = EmptyResult(mergeNode)

    planFor("MERGE (a {prop: 42})")._2 should equal(emptyResult)
  }

  test("should plan create followed by merge") {
    val createNode = CreateNode(Argument(), aId, Seq.empty, None)
    val allNodesScan = AllNodesScan(bId, Set.empty)
    val optional = Optional(ActiveRead(allNodesScan))
    val onCreate = MergeCreateNode(Argument(), bId, Seq.empty, None)
    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(bId))
    val apply = Apply(createNode, mergeNode)
    val emptyResult = EmptyResult(apply)

    planFor("CREATE (a) MERGE (b)")._2 should equal(emptyResult)
  }

  test("should plan merge followed by create") {
    val allNodesScan = AllNodesScan(aId, Set.empty)
    val optional = Optional(ActiveRead(allNodesScan))
    val onCreate = MergeCreateNode(Argument(), aId, Seq.empty, None)
    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId))
    val eager = Eager(mergeNode)
    val createNode = CreateNode(eager, bId, Seq.empty, None)
    val emptyResult = EmptyResult(createNode)

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
   *            /    \       arg2
   *       optional  set label
   *         /        \
   *    allnodes     arg1
   */
  test("should plan merge node with on create and on match ") {
    val allNodesScan = AllNodesScan(aId, Set.empty)
    val optional = Optional(ActiveRead(allNodesScan))
    val argument1 = Argument(Set(aId))
    val setLabels = SetLabels(argument1, aId, Seq(lblName("L")))
    val onMatch = ConditionalApply(optional, setLabels, Seq(aId))

    val argument2 = Argument()
    val mergeCreateNode = MergeCreateNode(argument2, aId, Seq.empty, None)
    val createAndOnCreate = SetNodeProperty(mergeCreateNode, aId, PropertyKeyName("prop")(pos), SignedDecimalIntegerLiteral("1")(pos))
    val mergeNode = AntiConditionalApply(onMatch, createAndOnCreate, Seq(aId))
    val emptyResult = EmptyResult(mergeNode)

    planFor("MERGE (a) ON CREATE SET a.prop = 1 ON MATCH SET a:L")._2 should equal(emptyResult)
  }
}
