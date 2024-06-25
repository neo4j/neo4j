/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.runtime.ast.ConstantExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.runtime.ast.TemporaryExpressionVariable
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.Result
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable
import scala.util.Success
import scala.util.Try

//noinspection NameBooleanParameters
class ExpressionVariableAllocationTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit private val idGen: IdGen = new SequentialIdGen()

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
    newPlan should be(projectPlan(withExpressionVariables(expr, TemporaryExpressionVariable(0, "x"))))
  }

  test("should un-cache cached properties") {
    val injectCachedNodeProperties: Rewriter = topDown(Rewriter.lift {
      case ci @ ContainerIndex(Variable("cache"), Property(v: Variable, pkn: PropertyKeyName)) =>
        CachedProperty(v, v, pkn, NODE_TYPE)(ci.position)
    })

    // given
    val expr = exprParser.parse("[ x IN [1,2,3] WHERE cache[x.foo] > 0 | cache[x.foo] + 1]").endoRewrite(
      injectCachedNodeProperties
    )
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(withExpressionVariables(
      exprParser.parse("[ x IN [1,2,3] WHERE x.foo > 0 | x.foo + 1]"),
      TemporaryExpressionVariable(0, "x")
    )))
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
      withExpressionVariables(exprX, TemporaryExpressionVariable(0, "x")),
      withExpressionVariables(exprY, TemporaryExpressionVariable(0, "y")),
      withExpressionVariables(exprZ, TemporaryExpressionVariable(0, "z"))
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
    newPlan should be(projectPlan(withExpressionVariables(
      expr,
      TemporaryExpressionVariable(0, "x"),
      TemporaryExpressionVariable(0, "y")
    )))
  }

  test("should replace nested expression variables") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | [ y IN [1,2,3] | y + x ] ]")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(2)
    newPlan should be(projectPlan(withExpressionVariables(
      expr,
      TemporaryExpressionVariable(0, "x"),
      TemporaryExpressionVariable(1, "y")
    )))
  }

  test("should replace independent nested expression variables") {
    // given
    val expr = exprParser.parse("[ x IN [1,2,3] | [ y IN [1,2,3] | y + x ] ++ [ z IN [1,2,3] | z + x ] ]")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(2)
    newPlan should be(projectPlan(withExpressionVariables(
      expr,
      TemporaryExpressionVariable(0, "x"),
      TemporaryExpressionVariable(1, "y"),
      TemporaryExpressionVariable(1, "z")
    )))
  }

  test("should replace expressions in all logical plans") {
    // given
    val exprX = exprParser.parse("all( x IN [1,2,3] WHERE x = 1)")
    val exprY = exprParser.parse("all( y IN [1,2,3] WHERE y = 1)")
    val exprZ = exprParser.parse("all( z IN [1,2,3] WHERE z = 1)")

    val selection = Selection(List(exprZ), Selection(List(exprY), Selection(List(exprX), Argument())))

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(selection)

    // then
    nSlots should be(1)
    newPlan should be(Selection(
      List(withExpressionVariables(exprZ, TemporaryExpressionVariable(0, "z"))),
      Selection(
        List(withExpressionVariables(exprY, TemporaryExpressionVariable(0, "y"))),
        Selection(List(withExpressionVariables(exprX, TemporaryExpressionVariable(0, "x"))), Argument())
      )
    ))
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
    val nestedPlanExpression = NestedPlanExpression.collect(nestedPlan, varFor("x1"), varFor("x1"))(pos)

    val outerExpression = allInList("y", nestedPlanExpression, exprParser.parse("[1,2,3]"))
    val outerPlan = projectPlan(outerExpression)

    // when
    val Result(newPlan, nSlots, availableExpressionVars) = expressionVariableAllocation.allocate(outerPlan)

    // then
    nSlots should be(3)
    newPlan should be(projectPlan(withExpressionVariables(
      outerExpression,
      TemporaryExpressionVariable(0, "y"),
      TemporaryExpressionVariable(1, "acc"),
      TemporaryExpressionVariable(2, "x")
    )))
    availableExpressionVars(nestedPlan.id) should be(Seq(TemporaryExpressionVariable(0, "y")))
  }

  test("should replace expressions in nested-nested plans") {
    // given
    val nestedNestedExpression = exprParser.parse("reduce(acc = 0, x IN [1,2,3] | acc + x )")
    val nestedNestedPlan = projectPlan(nestedNestedExpression)
    val nestedNestedPlanExpression = NestedPlanExpression.collect(nestedNestedPlan, varFor("x1"), varFor("x1"))(pos)

    val nestedExpression = allInList("yNested", nestedNestedPlanExpression, exprParser.parse("[1,2,3]"))
    val nestedPlan = projectPlan(nestedExpression)
    val nestedPlanExpression = NestedPlanExpression.collect(nestedPlan, varFor("x1"), varFor("x1"))(pos)

    val outerExpression = allInList("y", nestedPlanExpression, exprParser.parse("[1,2,3]"))
    val outerPlan = projectPlan(outerExpression)

    // when
    val Result(newPlan, nSlots, availableExpressionVars) = expressionVariableAllocation.allocate(outerPlan)

    // then
    nSlots should be(4)
    newPlan should be(projectPlan(withExpressionVariables(
      outerExpression,
      TemporaryExpressionVariable(0, "y"),
      TemporaryExpressionVariable(1, "yNested"),
      TemporaryExpressionVariable(2, "acc"),
      TemporaryExpressionVariable(3, "x")
    )))

    availableExpressionVars(nestedPlan.id) should be(Seq(TemporaryExpressionVariable(0, "y")))
    availableExpressionVars(nestedNestedPlan.id) should contain theSameElementsAs
      Seq(TemporaryExpressionVariable(0, "y"), TemporaryExpressionVariable(1, "yNested"))
  }

  test("nested plan with no available expression variables") {
    // given
    val nestedPlanExpression = NestedPlanExpression.collect(Argument(), varFor("y"), varFor("y"))(pos)
    val listComprehension = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val projection = projectPlan(nestedPlanExpression, listComprehension)

    // when
    val Result(newPlan, nSlots, availableExpressionVars) = expressionVariableAllocation.allocate(projection)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(
      nestedPlanExpression,
      withExpressionVariables(listComprehension, TemporaryExpressionVariable(0, "x"))
    ))
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
    newPlan should be(projectPlan(withExpressionVariables(
      expr,
      TemporaryExpressionVariable(0, "acc"),
      TemporaryExpressionVariable(1, "x")
    )))
  }

  test("should replace all iterable expression variable") {
    // given
    val expr = exprParser.parse("all(x IN [1,2,3] WHERE x = 2 )")
    val plan = projectPlan(expr)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(1)
    newPlan should be(projectPlan(withExpressionVariables(expr, TemporaryExpressionVariable(0, "x"))))
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
    val tempNode = TemporaryExpressionVariable(0, "tempNode")
    val tempEdge = TemporaryExpressionVariable(1, "tempEdge")
    newPlan should be(varLengthPlan(
      tempNode,
      tempEdge,
      withExpressionVariables(
        nodePred,
        tempNode,
        TemporaryExpressionVariable(2, "acc"),
        TemporaryExpressionVariable(3, "z")
      ),
      withExpressionVariables(edgePred, tempEdge)
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
    val tempNode = TemporaryExpressionVariable(0, "tempNode")
    val tempEdge = TemporaryExpressionVariable(1, "tempEdge")
    newPlan should be(pruningVarLengthPlan(
      tempNode,
      tempEdge,
      withExpressionVariables(
        nodePred,
        tempNode,
        TemporaryExpressionVariable(2, "acc"),
        TemporaryExpressionVariable(3, "z")
      ),
      withExpressionVariables(edgePred, tempEdge)
    ))
  }

  test("should replace variables in QueryConstant") {
    // given
    val plan =
      Selection(List(RuntimeConstant(varFor("A"), trueLiteral), RuntimeConstant(varFor("B"), falseLiteral)), Argument())

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(2)
    newPlan should be(Selection(
      List(
        RuntimeConstant(ConstantExpressionVariable(0, "A"), trueLiteral),
        RuntimeConstant(ConstantExpressionVariable(1, "B"), falseLiteral)
      ),
      Argument()
    ))
  }

  test("should handle a combination of constants and temporary variables") {
    // given
    val expr1 = exprParser.parse("[ x IN [1,2,3] | x + 1]")
    val expr2 = RuntimeConstant(varFor("A"), trueLiteral)
    val expr3 = exprParser.parse("[ y IN [1,2,3] | y + 1]")
    val expr4 = RuntimeConstant(varFor("B"), falseLiteral)
    val expr5 = exprParser.parse("[ z IN [1,2,3] | z + 1]")
    val plan = projectPlan(expr1, expr2, expr3, expr4, expr5)

    // when
    val Result(newPlan, nSlots, _) = expressionVariableAllocation.allocate(plan)

    // then
    nSlots should be(3)
    newPlan should be(projectPlan(
      withExpressionVariables(expr1, TemporaryExpressionVariable(2, "x")),
      withExpressionVariables(expr2, ConstantExpressionVariable(0, "A")),
      withExpressionVariables(expr3, TemporaryExpressionVariable(2, "y")),
      withExpressionVariables(expr4, ConstantExpressionVariable(1, "B")),
      withExpressionVariables(expr5, TemporaryExpressionVariable(2, "z"))
    ))
  }

  // ========== HELPERS ==========

  // all(varName IN list WHERE varName IN predicateList)
  private def allInList(varName: String, predicateList: Expression, list: Expression): AllIterablePredicate = {
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
      list
    )(pos)
  }

  private def projectPlan(exprs: Expression*): LogicalPlan = {
    val projections: Map[LogicalVariable, Expression] =
      (for (i <- exprs.indices) yield varFor(s"x$i") -> exprs(i)).toMap
    Projection(Argument(), projections)
  }

  private def varLengthPlan(
    tempNode: LogicalVariable,
    tempEdge: LogicalVariable,
    nodePred: Expression,
    edgePred: Expression
  ): LogicalPlan = {
    VarExpand(
      Argument(),
      varFor("a"),
      SemanticDirection.OUTGOING,
      SemanticDirection.OUTGOING,
      Seq.empty,
      varFor("b"),
      varFor("r"),
      VarPatternLength(2, Some(10)),
      ExpandAll,
      Seq(VariablePredicate(tempNode, nodePred)),
      Seq(VariablePredicate(tempEdge, edgePred))
    )
  }

  private def pruningVarLengthPlan(
    tempNode: LogicalVariable,
    tempEdge: LogicalVariable,
    nodePred: Expression,
    edgePred: Expression
  ): LogicalPlan = {
    PruningVarExpand(
      Argument(),
      varFor("a"),
      SemanticDirection.OUTGOING,
      Seq.empty,
      varFor("b"),
      2,
      10,
      Seq(VariablePredicate(tempNode, nodePred)),
      Seq(VariablePredicate(tempEdge, edgePred))
    )
  }

  private def withExpressionVariables(expression: Expression, exprVars: ExpressionVariable*): Expression = {
    val seen = mutable.Set[ExpressionVariable]()
    val rewritten = expression.endoRewrite(topDown(Rewriter.lift {
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

class ExpressionParser {

  def parse(text: String): Expression = {
    val defaultStatement = parse(CypherVersion.Default, text)

    // Quick and dirty hack to try to make sure we have sufficient coverage of all cypher versions.
    // Feel free to improve ¯\_(ツ)_/¯.
    CypherVersion.All.foreach { version =>
      if (version != CypherVersion.Default) {
        Try(parse(version, text)) match {
          case Success(otherStatement) if otherStatement == defaultStatement =>
          case notEqual => throw new AssertionError(
              s"""Unexpected result in $version
                 |Default statement: $defaultStatement
                 |$version statement: $notEqual
                 |""".stripMargin
            )
        }
      }
    }
    defaultStatement
  }

  def parse(version: CypherVersion, text: String): Expression =
    AstParserFactory(version)(text, Neo4jCypherExceptionFactory(text, None), None).expression()
}
