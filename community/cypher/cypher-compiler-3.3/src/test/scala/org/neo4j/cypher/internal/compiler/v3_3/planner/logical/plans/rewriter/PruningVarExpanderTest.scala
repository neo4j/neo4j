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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{IdName, VarPatternLength}

class PruningVarExpanderTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("simples possible query that can use PruningVarExpand") {
    // Simplest query:
    // match (a)-[*1..3]->(b) return distinct b

    val fromId = IdName("from")
    val allNodes = AllNodesScan(fromId, Set.empty)(solved)
    val dir = SemanticDirection.BOTH
    val length = VarPatternLength(1, Some(3))
    val toId = IdName("to")
    val relId = IdName("r")
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length)(solved)
    val input = Aggregation(originalExpand, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 1, 3)(solved)
    val expectedOutput = Aggregation(rewrittenExpand, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

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
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length)(solved)
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
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length)(solved)
    val predicate = HasLabels(Variable("to")(pos), Seq(LabelName("X")(pos)))(pos)
    val filter = Selection(Seq(predicate), originalExpand)(solved)
    val input = Aggregation(filter, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 1, 3)(solved)
    val filterAfterRewrite = Selection(Seq(predicate), rewrittenExpand)(solved)
    val expectedOutput = Aggregation(filterAfterRewrite, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

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
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length)(solved)
    val aggregate = Aggregation(originalExpand, Map.empty, Map("count" -> CountStar()(pos)))(solved)
    val input = Aggregation(aggregate, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

    rewrite(input) should equal(input)
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
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length)(solved)
    val originalExpand2 = VarExpand(originalExpand, bId, dir, dir, Seq(RelTypeName("T")(pos)), cId, relTId, length)(solved)
    val input = Aggregation(originalExpand2, Map("c" -> Variable("c")(pos)), Map.empty)(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, 1, 3)(solved)
    val rewrittenExpand2 = PruningVarExpand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, 1, 3)(solved)
    val expectedOutput = Aggregation(rewrittenExpand2, Map("c" -> Variable("c")(pos)), Map.empty)(solved)

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
    val originalExpand = VarExpand(allNodes, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length)(solved)
    val originalExpand2 = Expand(originalExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)(solved)
    val input = Aggregation(originalExpand2, Map("c" -> Variable("c")(pos)), Map.empty)(solved)

    val rewrittenExpand = PruningVarExpand(allNodes, aId, dir, Seq(RelTypeName("R")(pos)), bId, 1, 3)(solved)
    val rewrittenExpand2 = Expand(rewrittenExpand, bId, dir, Seq(RelTypeName("T")(pos)), cId, relTId)(solved)
    val expectedOutput = Aggregation(rewrittenExpand2, Map("c" -> Variable("c")(pos)), Map.empty)(solved)

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
    val argument = Argument(Set(aId))(solved)()
    val originalExpand = VarExpand(argument, aId, dir, dir, Seq(RelTypeName("R")(pos)), bId, relRId, length)(solved)
    val optional1 = Optional(originalExpand, Set(aId))(solved)
    val apply1 = Apply(allNodes, optional1)(solved)
    val distinct1 = Aggregation(apply1, Map("c" -> Variable("c")(pos)), Map.empty)(solved)

    val rewrittenExpand = PruningVarExpand(argument, aId, dir, Seq(RelTypeName("R")(pos)), bId, 1, 3)(solved)
    val optional2 = Optional(rewrittenExpand, Set(aId))(solved)
    val apply2 = Apply(allNodes, optional2)(solved)
    val distinct2 = Aggregation(apply2, Map("c" -> Variable("c")(pos)), Map.empty)(solved)

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
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length)(solved)
    val input = Aggregation(originalExpand, Map("r" -> Variable("r")(pos)), Map.empty)(solved)

    rewrite(input) should equal(input)
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
    val originalExpand = VarExpand(allNodes, fromId, dir, dir, Seq.empty, toId, relId, length)(solved)
    val input = Aggregation(originalExpand, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

    val rewrittenExpand = FullPruningVarExpand(allNodes, fromId, dir, Seq.empty, toId, 4, 5)(solved)
    val expectedOutput = Aggregation(rewrittenExpand, Map("to" -> Variable("to")(pos)), Map.empty)(solved)

    rewrite(input) should equal(expectedOutput)
  }

  private def rewrite(p: LogicalPlan): LogicalPlan =
    p.endoRewrite(pruningVarExpander)
}
