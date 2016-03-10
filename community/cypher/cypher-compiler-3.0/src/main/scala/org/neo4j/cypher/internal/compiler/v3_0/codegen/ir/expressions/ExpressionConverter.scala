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
package org.neo4j.cypher.internal.compiler.v3_0.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_0.codegen.ir.expressions
import org.neo4j.cypher.internal.compiler.v3_0.codegen.ir.functions.functionConverter
import org.neo4j.cypher.internal.compiler.v3_0.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.compiler.v3_0.planner.CantCompileQueryException
import org.neo4j.cypher.internal.frontend.v3_0.symbols.{CTNode, CTRelationship}
import org.neo4j.cypher.internal.frontend.v3_0.{InternalException, ast, symbols}

object ExpressionConverter {

  implicit class ExpressionToPredicate(expression: CodeGenExpression) {

    def asPredicate = new CodeGenExpression {

      override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
        if (expression.nullable) structure.coerceToBoolean(expression.generateExpression(structure))
        else expression.generateExpression(structure)
      }

      override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = expression.init(generator)

      override def nullable(implicit context: CodeGenContext) = false

      override def cypherType(implicit context: CodeGenContext) = symbols.CTBoolean
    }
  }

  def createPredicate(expression: ast.Expression)
                     (implicit context: CodeGenContext): CodeGenExpression = expression match {
    case ast.HasLabels(ast.Variable(name), label :: Nil) =>
      val labelIdVariable = context.namer.newVarName()
      val nodeVariable = context.getVariable(name)
      HasLabel(nodeVariable, labelIdVariable, label.name).asPredicate

    case exp@ast.Property(node@ast.Variable(name), propKey) if context.semanticTable.isNode(node) =>
      createExpression(exp).asPredicate

    case exp@ast.Property(node@ast.Variable(name), propKey) if context.semanticTable.isRelationship(node) =>
      createExpression(exp).asPredicate

    case ast.Not(e) => Not(createExpression(e)).asPredicate

    case ast.Equals(lhs, rhs) => Equals(createExpression(lhs), createExpression(rhs)).asPredicate

    case ast.Or(lhs, rhs) => Or(createExpression(lhs), createExpression(rhs)).asPredicate

    case other =>
      throw new CantCompileQueryException(s"Predicate of $other not yet supported")

  }

  def createExpression(expression: ast.Expression)
                      (implicit context: CodeGenContext): CodeGenExpression = expressionConverter(expression, createExpression)

  def createProjection(expression: ast.Expression)
                      (implicit context: CodeGenContext): CodeGenExpression = {

    expression match {
      case node@ast.Variable(name) if context.semanticTable.isNode(node) =>
        NodeProjection(context.getVariable(name))

      case rel@ast.Variable(name) if context.semanticTable.isRelationship(rel) =>
        RelationshipProjection(context.getVariable(name))

      case e => expressionConverter(e, createProjection)
    }
  }

  def createExpressionForVariable(variableQueryVariable: String)
                                 (implicit context: CodeGenContext): CodeGenExpression = {

    val variable = context.getVariable(variableQueryVariable)

    variable.cypherType match {
      case CTNode => NodeProjection(variable)
      case CTRelationship => RelationshipProjection(variable)
      case _ => throw new InternalException("The compiled runtime only handles variables pointing to rels and nodes at this time")
    }
  }

  private def expressionConverter(expression: ast.Expression, callback: ast.Expression => CodeGenExpression)
                      (implicit context: CodeGenContext): CodeGenExpression = {

    expression match {
      case node@ast.Variable(name) if context.semanticTable.isNode(node) =>
        NodeExpression(context.getVariable(name))

      case rel@ast.Variable(name) if context.semanticTable.isRelationship(rel) =>
        RelationshipExpression(context.getVariable(name))

      case ast.Property(node@ast.Variable(name), propKey) if context.semanticTable.isNode(node) =>
        val token = propKey.id(context.semanticTable).map(_.id)
        NodeProperty(token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case ast.Property(rel@ast.Variable(name), propKey) if context.semanticTable.isRelationship(rel) =>
        val token = propKey.id(context.semanticTable).map(_.id)
        RelProperty(token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case ast.Parameter(name, _) => expressions.Parameter(name, context.namer.newVarName())

      case lit: ast.IntegerLiteral => Literal(lit.value)

      case lit: ast.DoubleLiteral => Literal(lit.value)

      case lit: ast.StringLiteral => Literal(lit.value)

      case lit: ast.Literal => Literal(lit.value)

      case ast.Collection(exprs) =>
        expressions.Collection(exprs.map(e => callback(e)))

      case ast.Add(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Addition(leftOp, rightOp)

      case ast.Subtract(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Subtraction(leftOp, rightOp)

      case ast.MapExpression(items) =>
        val map = items.map {
          case (key, expr) => (key.name, callback(expr))
        }.toMap
        MyMap(map)

      case ast.HasLabels(ast.Variable(name), label :: Nil) =>
        val labelIdVariable = context.namer.newVarName()
        val nodeVariable = context.getVariable(name)
        HasLabel(nodeVariable, labelIdVariable, label.name)

      case ast.Equals(lhs, rhs) => Equals(callback(lhs), callback(rhs))

      case ast.Or(lhs, rhs) => Or(callback(lhs), callback(rhs))

      case ast.Not(inner) => Not(callback(inner))

      case f: ast.FunctionInvocation => functionConverter(f, callback)

      case other => throw new CantCompileQueryException(s"Expression of $other not yet supported")
    }
  }

}
