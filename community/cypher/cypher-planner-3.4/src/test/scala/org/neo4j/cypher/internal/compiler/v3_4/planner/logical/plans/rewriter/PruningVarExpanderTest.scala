/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4.{IdName, VarPatternLength}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans._

class PruningVarExpanderTest extends CypherFunSuite with LogicalPlanningTestSupport with AstConstructionTestSupport {
  test("simplest possible query that can use PruningVarExpand") {
    // Simplest query:
    // match (a)-[*1..3]->(b) return distinct b

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val input = Distinct(originalExpand, Map("to" -> Variable("to")(pos)))(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 1, 3)(solved)
    val expectedOutput = Distinct(rewrittenExpand, Map("to" -> Variable("to")(pos)))(solved)

    rewrite(input) should equal(expectedOutput)
  }

  test("query with distinct aggregation") {
    // Simplest query:
    // match (from)-[*1..3]->(to) return count(distinct to)

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val aggregatingExpression = FunctionInvocation(functionName = FunctionName("count")(pos), distinct = true, args = IndexedSeq(Variable("to")(pos)))(pos)
    val input = Aggregation(originalExpand, Map.empty, Map("x" -> aggregatingExpression))(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 1, 3)(solved)
    val expectedOutput = Aggregation(rewrittenExpand, Map.empty, Map("x" -> aggregatingExpression))(solved)

    rewrite(input) should equal(expectedOutput)
  }

  test("Simple query that filters between expand and distinct") {
    // Simplest query:
    // match (a)-[*1..3]->(b:X) return distinct b

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val predicate = HasLabels(Variable("to")(pos), Seq(LabelName("X")(pos)))(pos)
    val filter = Selection(Seq(predicate), originalExpand)(solved)
    val input = Distinct(filter, Map("to" -> Variable("to")(pos)))(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 1, 3)(solved)
    val filterAfterRewrite = Selection(Seq(predicate), rewrittenExpand)(solved)
    val expectedOutput = Distinct(filterAfterRewrite, Map("to" -> Variable("to")(pos)))(solved)

    rewrite(input) should equal(expectedOutput)
  }

  test("Query that aggregates before making the result DISTINCT") {
    // Simplest query:
    // match (a)-[:R*1..3]->(b) with count(*) as count return distinct count

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val aggregate = Aggregation(originalExpand, Map.empty, Map("count" -> CountStar()(pos)))(solved)
    val input = Distinct(aggregate, Map("to" -> Variable("to")(pos)))(solved)

    assertNotRewritten(input)
  }

  test("Double var expand with distinct result") {
    // Simplest query:
    // match (a)-[:R*1..3]->(b)-[:T*1..3]->(c) return distinct c

    val aId = IdName("a")
    val relRId = IdName("r")
    val bId = IdName("b")
    val relTId = IdName("t")
    val cId = IdName("c")
    val allNodes = AllNodesScan(aId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val originalExpand2 = VarExpand(originalExpand, bId, dir, dir, Seq(RelTypeName("T")(pos)), cId, relTId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val input = Distinct(originalExpand2, Map("c" -> Variable("c")(pos)))(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, 1, 3)(solved)
    val rewrittenExpand2 = PruningVarExpand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, 1, 3)(solved)
    val expectedOutput = Distinct(rewrittenExpand2, Map("c" -> Variable("c")(pos)))(solved)

    rewrite(input) should equal(expectedOutput)
  }

  test("var expand followed by normal expand") {
    // Simplest query:
    // match (a)-[:R*1..3]->(b)-[:T]->(c) return distinct c

    val aId = IdName("a")
    val relRId = IdName("r")
    val bId = IdName("b")
    val relTId = IdName("t")
    val cId = IdName("c")
    val allNodes = AllNodesScan(aId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val originalExpand2 = Expand(originalExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)(solved)
    val input = Distinct(originalExpand2, Map("c" -> Variable("c")(pos)))(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, 1, 3)(solved)
    val rewrittenExpand2 = Expand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)(solved)
    val expectedOutput = Distinct(rewrittenExpand2, Map("c" -> Variable("c")(pos)))(solved)

    rewrite(input) should equal(expectedOutput)
  }

  test("optional match can be solved with PruningVarExpand") {
    /* Simplest query:
       match (a) optional match (a)-[:R*1..3]->(b)-[:T]->(c) return distinct c
       in logical plans:

              distinct1                            distinct2
                 |                                    |
               apply1                               apply2
               /   \                                /   \
       all-nodes   optional1      ===>       all-nodes  optional2
                     \                                    \
                     var-length-expand                   pruning-var-expand
                       \                                    \
                       argument                            argument
    */

    val aId = IdName("a")
    val relRId = IdName("r")
    val bId = IdName("b")
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))

    val allNodes = AllNodesScan(aId, Set.empty)(solved)
    val argument = Argument(Set(aId))(solved)
    val originalExpand = VarExpand(argument, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val optional1 = Optional(originalExpand, Set(aId))(solved)
    val apply1 = Apply(allNodes, optional1)(solved)
    val distinct1 = Distinct(apply1, Map("c" -> Variable("c")(pos)))(solved)

    val rewrittenExpand = PruningVarExpand(argument, aId, dir, Seq(RelTypeName("R")(pos)), bId, 1, 3)(solved)
    val optional2 = Optional(rewrittenExpand, Set(aId))(solved)
    val apply2 = Apply(allNodes, optional2)(solved)
    val distinct2 = Distinct(apply2, Map("c" -> Variable("c")(pos)))(solved)

    rewrite(distinct1) should equal(distinct2)
  }

  test("should not rewrite when doing non-distinct aggregation") {
    // Should not be rewritten since it's asking for a count of all paths leading to a node
    // match (a)-[*1..3]->(b) return b, count(*)

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val input = Aggregation(originalExpand, Map("r" -> Variable("r")(pos)), Map.empty)(solved)

    assertNotRewritten(input)
  }

  test("on longer var-lengths, we use FullPruningVarExpand") {
    // Simplest query:
    // match (a)-[*4..5]->(b) return distinct b

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(4, Some(5))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val input = Distinct(originalExpand, Map("to" -> Variable("to")(pos)))(solved)

    val rewrittenExpand = FullPruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 4, 5)(solved)
    val expectedOutput = Distinct(rewrittenExpand, Map("to" -> Variable("to")(pos)))(solved)

    rewrite(input) should equal(expectedOutput)
  }

  test("do not use pruning for length=1") {
    // Simplest query:
    // match (a)-[*1..1]->(b) return distinct b

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(1))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val input = Distinct(originalExpand, Map("to" -> Variable("to")(pos)))(solved)

    assertNotRewritten(input)
  }

  test("do not use pruning for pathExpressions when path is needed") {
    // Simplest query:
    // match p=(from)-[r*0..1]->(to) with nodes(p) as d return distinct d

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(0, Some(2))
    val toId = IdName("to")

    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandAll, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val pathProjectior = NodePathStep(varFor("from"), MultiRelationshipPathStep(varFor("r"), SemanticDirection.BOTH, NilPathStep))

    val function = FunctionInvocation(FunctionName("nodes")(pos), PathExpression(pathProjectior)(pos))(pos)
    val projection = Projection(originalExpand, Map("d" -> function))(solved)
    val distinct = Distinct(projection, Map("d" -> varFor("d")))(solved)

    assertNotRewritten(distinct)
  }

  test("do not use pruning-varexpand when both sides of the var-length-relationship are already known") {
    val fromId = IdName("from")
    val toId = IdName("to")
    val fromPlan = AllNodesScan(fromId, Set.empty)(solved)
    val toPlan = AllNodesScan(toId, Set.empty)(solved)
    val xJoin = CartesianProduct(fromPlan, toPlan)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val relId = IdName("r")
    val originalExpand = VarExpand(xJoin, fromId, dir, dir, Seq.empty, toId, relId, length, ExpandInto, IdName("tempNode"), IdName("tempEdge"), TRUE, TRUE, Seq.empty)(solved)
    val input = Aggregation(originalExpand, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

    assertNotRewritten(input)
  }

  test("should handle insanely long logical plans without running out of stack") {
    val leafPlan: LogicalPlan = Argument(Set(IdName("x")))(solved)
    var plan = leafPlan
    (1 until 10000) foreach { i =>
      plan = Selection(Seq(True()(pos)), plan)(solved)
    }

    rewrite(plan) // should not throw exception
  }

  private def assertNotRewritten(p: LogicalPlan): Unit = {
    rewrite(p) should equal(p)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(pruningVarExpander)
}
