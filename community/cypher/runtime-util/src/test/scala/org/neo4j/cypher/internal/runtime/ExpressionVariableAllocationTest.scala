/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.Result
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.{Expression, _}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.parser.Expressions
import org.neo4j.cypher.internal.v4_0.util.attribution.{IdGen, SequentialIdGen}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}
import org.parboiled.scala.{ReportingParseRunner, Rule1}

import scala.collection.mutable

//noinspection NameBooleanParameters
class ExpressionVariableAllocationTest extends CypherFunSuite with AstConstructionTestSupport {

  private implicit val idGen: IdGen = new SequentialIdGen()

  val exprParser = new ExpressionParser

  test("should noop for regular variable") {
    // given
    val plan = Selection(List(varFor("x")), Argument())

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(0)
    newPlan should be(plan)
  }

  test("should replace expression variable") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(withExpressionVariables(expr, ExpressionVariable(0, "x"))))
  }

  test("should un-cache cached properties") {
    val injectCachedNodeProperties: Rewriter = topDown(Rewriter.lift {
      case ci@ContainerIndex(Variable("cache"), Property(v@Variable(node), pkn:PropertyKeyName)) =>
        CachedProperty(node, v, pkn, NODE_TYPE)(ci.position)
    })

    // given
    val expr = exprParser.parse("[ x IN [1,2,3] WHERE cache[x.foo] > 0 | cache[x.foo] + 1]").endoRewrite(injectCachedNodeProperties)
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(withExpressionVariables(exprParser.parse("[ x IN [1,2,3] WHERE x.foo > 0 | x.foo + 1]"), ExpressionVariable(0, "x"))))
  }

  test("should replace independent expression variables") {
    // given
    val exprX = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val exprY = exprParser.parse("[ y IN [1,2,3] | y + 1]")
    val exprZ = exprParser.parse("[ z IN [1,2,3] | z + 1]")
    val plan = projectPlan(exprX, exprY, exprZ)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(
      withExpressionVariables(exprX, ExpressionVariable(0, "x")),
      withExpressionVariables(exprY, ExpressionVariable(0, "y")),
      withExpressionVariables(exprZ, ExpressionVariable(0, "z"))
    ))
  }

  test("should replace independent expression variables II") {
    // given
    val expr = exprParser.parse("[ x IN [ y IN [1,2,3] | y + 1] | x + 2]")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"),
                                                          ExpressionVariable(0, "y"))))
  }

  test("should replace nested expression variables") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | [ y IN [1,2,3] | y + x ] ]")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(2)
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"),
                                                          ExpressionVariable(1, "y"))))
  }

  test("should replace independent nested expression variables") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | [ y IN [1,2,3] | y + x ] ++ [ z IN [1,2,3] | z + x ] ]")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(2)
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"),
                                                          ExpressionVariable(1, "y"),
                                                          ExpressionVariable(1, "z"))))
  }

  test("should replace expressions in all logical plans") {
    // given
    val exprX = exprParser.parse("all( x IN [1,2,3] WHERE x = 1)")
    val exprY = exprParser.parse("all( y IN [1,2,3] WHERE y = 1)")
    val exprZ = exprParser.parse("all( z IN [1,2,3] WHERE z = 1)")

    val selection = Selection(List(exprZ),
                              Selection(List(exprY),
                                        Selection(List(exprX),
                                                  Argument())))

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(selection)

    // then
    nSlots should be(1)
    newPlan should be(Selection(List(withExpressionVariables(exprZ, ExpressionVariable(0, "z"))),
                                Selection(List(withExpressionVariables(exprY, ExpressionVariable(0, "y"))),
                                          Selection(List(withExpressionVariables(exprX, ExpressionVariable(0, "x"))),
                                                    Argument()))))
  }

  test("should replace expressions in nested plans") {

    /*

    legend:
    -------------
    LOGICAL PLAN
    expression

    the test encodes a nested plan like this:

       PROJECTION
      /         \
    ARGUMENT  all(y IN [1,2,3] WHERE y IN _)
                 |
              nestedPlan
                 |
            PROJECTION
             /      \
       ARGUMENT    reduce(acc = 0, x IN [1,2,3] | acc + x )

     */

    // given
    val nestedExpression = exprParser.parse("reduce(acc = 0, x IN [1,2,3] | acc + x )")
    val nestedPlan = projectPlan(nestedExpression)
    val nestedPlanExpression = NestedPlanExpression(nestedPlan, varFor("x1"))(pos)

    val outerExpression = allInList("y", nestedPlanExpression, exprParser.parse("[1,2,3]"))
    val outerPlan = projectPlan(outerExpression)

    // when
    val Result(newPlan, nSlots, availableExpressionVars) = expressionVariableAllocation.allocate(outerPlan)

    // then
    nSlots should be(3)
    newPlan should be(projectPlan(withExpressionVariables(outerExpression,
                                                          ExpressionVariable(0, "y"),
                                                          ExpressionVariable(1, "acc"),
                                                          ExpressionVariable(2, "x"))))
    availableExpressionVars(nestedPlan.id) should be(Seq(ExpressionVariable(0, "y")))
  }

  test("should replace expressions in nested-nested plans") {
    // given
    val nestedNestedExpression = exprParser.parse("reduce(acc = 0, x IN [1,2,3] | acc + x )")
    val nestedNestedPlan = projectPlan(nestedNestedExpression)
    val nestedNestedPlanExpression = NestedPlanExpression(nestedNestedPlan, varFor("x1"))(pos)

    val nestedExpression = allInList("yNested", nestedNestedPlanExpression, exprParser.parse("[1,2,3]"))
    val nestedPlan = projectPlan(nestedExpression)
    val nestedPlanExpression = NestedPlanExpression(nestedPlan, varFor("x1"))(pos)

    val outerExpression = allInList("y", nestedPlanExpression, exprParser.parse("[1,2,3]"))
    val outerPlan = projectPlan(outerExpression)

    // when
    val Result(newPlan, nSlots, availableExpressionVars) = expressionVariableAllocation.allocate(outerPlan)

    // then
    nSlots should be(4)
    newPlan should be(projectPlan(withExpressionVariables(outerExpression,
                                                          ExpressionVariable(0, "y"),
                                                          ExpressionVariable(1, "yNested"),
                                                          ExpressionVariable(2, "acc"),
                                                          ExpressionVariable(3, "x"))))

    availableExpressionVars(nestedPlan.id) should be(Seq(ExpressionVariable(0, "y")))
    availableExpressionVars(nestedNestedPlan.id) should contain theSameElementsAs
      Seq(ExpressionVariable(0, "y"), ExpressionVariable(1, "yNested"))
  }

  test("nested plan with no available expression variables") {
    // given
    val nestedPlanExpression = NestedPlanExpression(Argument(), varFor("y"))(pos)
    val listComprehension = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val projection = projectPlan(nestedPlanExpression, listComprehension)

    // when
    val Result(newPlan, nSlots, availableExpressionVars) = expressionVariableAllocation.allocate(projection)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(nestedPlanExpression, withExpressionVariables(listComprehension,
                                                          ExpressionVariable(0, "x"))))
    availableExpressionVars(nestedPlanExpression.plan.id) should be(Seq.empty)
  }

  test("should replace both reduce expression variables") {
    // given
    val expr = exprParser.parse("reduce(acc = 0, x IN [1,2,3] | acc + x )")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(2)
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "acc"),
                                                          ExpressionVariable(1, "x"))))
  }

  test("should replace all iterable expression variable") {
    // given
    val expr = exprParser.parse("all(x IN [1,2,3] WHERE x = 2 )")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(withExpressionVariables(expr,
                                                          ExpressionVariable(0, "x"))))
  }

  test("should replace var-length expression variables") {
    // given
    val nodePred = exprParser.parse("reduce(acc = true, z IN tempNode.prop | acc AND z )")
    val edgePred = exprParser.parse("tempEdge = true")
    val plan = varLengthPlan(varFor("tempNode"), varFor("tempEdge"), nodePred, edgePred)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(4)
    val tempNode = ExpressionVariable(0, "tempNode")
    val tempEdge = ExpressionVariable(1, "tempEdge")
    newPlan should be(varLengthPlan(tempNode,
                                    tempEdge,
                                    withExpressionVariables(nodePred,
                                                            tempNode,
                                                            ExpressionVariable(2, "acc"),
                                                            ExpressionVariable(3, "z")),
                                    withExpressionVariables(edgePred,
                                                            tempEdge)
    ))
  }

  test("should replace pruning var-length expression variables") {
    // given
    val nodePred = exprParser.parse("reduce(acc = true, z IN tempNode.prop | acc AND z )")
    val edgePred = exprParser.parse("tempEdge = true")
    val plan = pruningVarLengthPlan(varFor("tempNode"), varFor("tempEdge"), nodePred, edgePred)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(4)
    val tempNode = ExpressionVariable(0, "tempNode")
    val tempEdge = ExpressionVariable(1, "tempEdge")
    newPlan should be(pruningVarLengthPlan(tempNode,
                                           tempEdge,
                                           withExpressionVariables(nodePred,
                                                                   tempNode,
                                                                   ExpressionVariable(2, "acc"),
                                                                   ExpressionVariable(3, "z")),
                                           withExpressionVariables(edgePred,
                                                                   tempEdge)
    ))
  }

  // ========== HELPERS ==========

  // all(varName IN list WHERE varName IN predicateList)
  private def allInList(varName: String,
                        predicateList: Expression,
                        list: Expression): AllIterablePredicate = {
    AllIterablePredicate(
      FilterScope(
        varFor(varName),
        Some(
          In(
            varFor(varName),
            predicateList
          )(pos)
        )
      )(pos),
      list)(pos)
  }

  private def projectPlan(exprs: Expression*): LogicalPlan = {
    val projections = (for (i <- exprs.indices) yield s"x$i" -> exprs(i)).toMap
    Projection(Argument(), projections)
  }

  private def varLengthPlan(tempNode: LogicalVariable,
                            tempEdge: LogicalVariable,
                            nodePred: Expression,
                            edgePred: Expression): LogicalPlan = {
    VarExpand(Argument(),
              "a",
              SemanticDirection.OUTGOING,
              SemanticDirection.OUTGOING,
              Seq.empty,
              "b",
              "r",
              VarPatternLength(2, Some(10)),
              ExpandAll,
              Some(VariablePredicate(tempNode, nodePred)),
              Some(VariablePredicate(tempEdge, edgePred)))
  }

  private def pruningVarLengthPlan(tempNode: LogicalVariable,
                                   tempEdge: LogicalVariable,
                                   nodePred: Expression,
                                   edgePred: Expression): LogicalPlan = {
    PruningVarExpand(Argument(),
                     "a",
                     SemanticDirection.OUTGOING,
                     Seq.empty,
                     "b",
                     2,
                     10,
                     Some(VariablePredicate(tempNode, nodePred)),
                     Some(VariablePredicate(tempEdge, edgePred)))
  }

  private def withExpressionVariables(expression: Expression, exprVars: ExpressionVariable*): Expression = {
    val seen = mutable.Set[ExpressionVariable]()
    val rewritten = expression.endoRewrite(topDown( Rewriter.lift {
      case x: LogicalVariable =>
        exprVars.find(_.name == x.name) match {
          case Some(exprVar) =>
            seen += exprVar
            exprVar
          case None => x
        }
    }))
    val unseen = exprVars.toSet -- seen
    if (unseen.nonEmpty) {
      fail("Did not replace expression variables\n" + unseen.mkString("\n"))
    }
    rewritten
  }
}

class ExpressionParser extends Expressions {
  private val parser: Rule1[Expression] = Expression

  def parse(text: String): Expression = {
    val res = ReportingParseRunner(parser).run(text)
    res.result match {
      case Some(e) => e
      case None => throw new IllegalArgumentException(s"Could not parse expression: ${res.parseErrors}")
    }
  }
}
