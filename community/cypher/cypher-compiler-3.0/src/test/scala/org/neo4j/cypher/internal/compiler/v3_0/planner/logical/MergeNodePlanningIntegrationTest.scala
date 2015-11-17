/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v3_0.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.ast.{LabelName, HasLabels, Variable, Collection, In, LabelToken, Property, PropertyKeyName, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite


class MergeNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single merge node") {
    val allNodesScan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val mergeNode = MergeNode(optional, IdName("a"), Seq.empty, Map.empty, Seq.empty, Seq.empty)(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a)").plan should equal(emptyResult)
  }

  test("should plan single merge node from a label scan") {

    val labelScan = NodeByLabelScan(IdName("a"), LazyLabel("X"), Set.empty)(solved)
    val optional = Optional(labelScan)(solved)
    val mergeNode = MergeNode(optional, IdName("a"), Seq(LazyLabel("X")), Map.empty, Seq.empty, Seq.empty)(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    (new given {
      labelCardinality = Map(
        "X" -> 30.0
      )
    } planFor "MERGE (a:X)").plan should equal(emptyResult)
  }

  test("should plan single merge node with properties") {

    val allNodesScan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val propertyKeyName = PropertyKeyName("prop")(pos)
    val propertyValue = SignedDecimalIntegerLiteral("42")(pos)
    val selection = Selection(Seq(In(Property(Variable("a")(pos), propertyKeyName)(pos),
      Collection(Seq(propertyValue))(pos))(pos)), allNodesScan)(solved)
    val optional = Optional(selection)(solved)
    val mergeNode = MergeNode(optional, IdName("a"), Seq.empty, Map(propertyKeyName -> propertyValue), Seq.empty, Seq.empty)(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a {prop: 42})").plan should equal(emptyResult)
  }

  test("should plan create followed by merge") {
    val createNode = CreateNode(SingleRow()(solved), IdName("a"), Seq.empty, None)(solved)
    val allNodesScan = AllNodesScan(IdName("b"), Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val apply = Apply(createNode, optional)(solved)
    val mergeNode = MergeNode(apply, IdName("b"), Seq.empty, Map.empty, Seq.empty, Seq.empty)(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("CREATE (a) MERGE (b)").plan should equal(emptyResult)
  }

  test("should plan merge followed by create") {
    val allNodesScan = AllNodesScan(IdName("a"), Set.empty)(solved)
    val optional = Optional(allNodesScan)(solved)
    val mergeNode = MergeNode(optional, IdName("a"), Seq.empty, Map.empty, Seq.empty, Seq.empty)(solved)
    val createNode = CreateNode(mergeNode, IdName("b"), Seq.empty, None)(solved)
    val emptyResult = EmptyResult(createNode)(solved)

    planFor("MERGE(a) CREATE (b)").plan should equal(emptyResult)
  }

  test("should use AssertSameNode when multiple unique index matches") {
    (new given {
      uniqueIndexOn("X", "prop")
      uniqueIndexOn("Y", "prop")
    } planFor "MERGE (a:X:Y {prop: 42})").plan should beLike {
      case EmptyResult(
            MergeNode(
              Optional(
              AssertSameNode(IdName("a"),
                NodeUniqueIndexSeek(IdName("a"), LabelToken("X", _), _, _, _),
                NodeUniqueIndexSeek(IdName("a"), LabelToken("Y", _), _, _, _)
              )), _, Seq(LazyLabel("X"), LazyLabel("Y")), _, _, _)) => ()
    }
  }

  test("should not use AssertSameNode when multiple unique index matches") {
    (new given {
      uniqueIndexOn("X", "prop")
    } planFor "MERGE (a:X:Y {prop: 42})").plan should not be using[AssertSameNode]
  }
}
