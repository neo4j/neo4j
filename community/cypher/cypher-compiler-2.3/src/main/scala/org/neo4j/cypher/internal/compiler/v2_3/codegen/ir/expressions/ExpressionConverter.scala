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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ast
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.codegen.CodeGenContext
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantCompileQueryException

object ExpressionConverter {
  def createPredicate(expression: Expression)
                     (implicit context: CodeGenContext): CodeGenExpression = expression match {
    case HasLabels(Identifier(name), label :: Nil) =>
      val labelIdVariable = context.namer.newVarName()
      val nodeVariable = context.getVariable(name)
      HasLabelPredicate(nodeVariable, labelIdVariable, label.name)

    case exp@Property(node@Identifier(name), propKey) if context.semanticTable.isNode(node) =>
      PropertyAsPredicate(createExpression(exp))

    case exp@Property(node@Identifier(name), propKey) if context.semanticTable.isRelationship(node) =>
      PropertyAsPredicate(createExpression(exp))

    case other =>
      throw new CantCompileQueryException(s"Predicate of $other not yet supported")

  }

  def createExpression(expression: Expression)
                      (implicit context: CodeGenContext): CodeGenExpression = {

    expression match {
      case node@Identifier(name) if context.semanticTable.isNode(node) =>
        Node(context.getVariable(name))

      case rel@Identifier(name) if context.semanticTable.isRelationship(rel) =>
        Relationship(context.getVariable(name))

      case Property(node@Identifier(name), propKey) if context.semanticTable.isNode(node) =>
        val token = propKey.id(context.semanticTable).map(_.id)
        NodeProperty(token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case Property(rel@Identifier(name), propKey) if context.semanticTable.isRelationship(rel) =>
        val token = propKey.id(context.semanticTable).map(_.id)
        RelProperty(token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case ast.Parameter(name) => expressions.Parameter(name)

      case lit: IntegerLiteral => Literal(lit.value)

      case lit: DoubleLiteral => Literal(lit.value)

      case lit: StringLiteral => Literal(lit.value)

      case lit: ast.Literal => Literal(lit.value)

      case ast.Collection(exprs) =>
        expressions.Collection(exprs.map(e => createExpression(e)))

      case Add(lhs, rhs) =>
        val leftOp = createExpression(lhs)
        val rightOp = createExpression(rhs)
        Addition(leftOp, rightOp)

      case Subtract(lhs, rhs) =>
        val leftOp = createExpression(lhs)
        val rightOp = createExpression(rhs)
        Subtraction(leftOp, rightOp)

      case MapExpression(items: Seq[(PropertyKeyName, Expression)]) =>
        val map = items.map {
          case (key, expr) => (key.name, createExpression(expr))
        }.toMap
        MyMap(map)

      case HasLabels(Identifier(name), label :: Nil) =>
        val labelIdVariable = context.namer.newVarName()
        val nodeVariable = context.getVariable(name)
        HasLabel(nodeVariable, labelIdVariable, label.name)

      case other => throw new CantCompileQueryException(s"Expression of $other not yet supported")
    }
  }
}
