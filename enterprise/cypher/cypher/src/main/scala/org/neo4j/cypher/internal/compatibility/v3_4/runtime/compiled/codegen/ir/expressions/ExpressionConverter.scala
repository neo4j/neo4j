/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.functions.functionConverter
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.helpers.LiteralTypeSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.CantCompileQueryException
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.{expressions => ast}

object ExpressionConverter {

  implicit class ExpressionToPredicate(expression: CodeGenExpression) {

    def asPredicate = new CodeGenExpression {

      override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
        if (expression.nullable || !expression.codeGenType.isPrimitive) structure.coerceToBoolean(expression.generateExpression(structure))
        else expression.generateExpression(structure)
      }

      override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = expression.init(generator)

      override def nullable(implicit context: CodeGenContext) = false

      override def codeGenType(implicit context: CodeGenContext) =
        if (nullable) CypherCodeGenType(CTBoolean, ReferenceType)
        else expression.codeGenType
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

    case exp: ast.Variable =>
      createExpression(exp).asPredicate

    case _:ast.False => False
    case _:ast.True => True

    case other =>
      throw new CantCompileQueryException(s"Predicate of $other not yet supported")

  }

  def createExpression(expression: ast.Expression)
                      (implicit context: CodeGenContext): CodeGenExpression = expressionConverter(expression, createExpression)

  def createMaterializeExpressionForVariable(variableQueryVariable: String)
                                            (implicit context: CodeGenContext): CodeGenExpression = {

    val variable = context.getVariable(variableQueryVariable)

    variable.codeGenType match {
      case CypherCodeGenType(CTNode, _) => NodeProjection(variable)
      case CypherCodeGenType(CTRelationship, _) => RelationshipProjection(variable)
      case CypherCodeGenType(CTString, _) |
           CypherCodeGenType(CTBoolean, _) |
           CypherCodeGenType(CTInteger, _) |
           CypherCodeGenType(CTFloat, _) =>
        LoadVariable(variable)
      case CypherCodeGenType(ListType(CTInteger), ListReferenceType(LongType)) =>
        // TODO: PrimitiveProjection(variable)
        AnyProjection(variable) // Temporarily resort to runtime projection
      case CypherCodeGenType(ListType(CTFloat), ListReferenceType(FloatType)) =>
        // TODO: PrimitiveProjection(variable)
        AnyProjection(variable) // Temporarily resort to runtime projection
      case CypherCodeGenType(ListType(CTBoolean), ListReferenceType(BoolType)) =>
        // TODO: PrimitiveProjection(variable)
        AnyProjection(variable) // Temporarily resort to runtime projection
      case CypherCodeGenType(ListType(CTString), _) |
           CypherCodeGenType(ListType(CTBoolean), _) |
           CypherCodeGenType(ListType(CTInteger), _) |
           CypherCodeGenType(ListType(CTFloat), _) =>
        LoadVariable(variable)
      case CypherCodeGenType(CTAny, _) => AnyProjection(variable)
      case CypherCodeGenType(CTMap, _) => AnyProjection(variable)
      case CypherCodeGenType(ListType(_), _) => AnyProjection(variable) // TODO: We could have a more specialized projection when the inner type is known to be node or relationship
      case _ => throw new CantCompileQueryException(s"The compiled runtime cannot handle results of type ${variable.codeGenType}")
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
        val token = context.semanticTable.id(propKey).map(_.id)
        NodeProperty(token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case ast.Property(rel@ast.Variable(name), propKey) if context.semanticTable.isRelationship(rel) =>
        val token = context.semanticTable.id(propKey).map(_.id)
        RelProperty(token, propKey.name, context.getVariable(name), context.namer.newVarName())

      case ast.Property(mapExpression, ast.PropertyKeyName(propKeyName)) =>
        MapProperty(callback(mapExpression), propKeyName)

      case ast.Parameter(name, cypherType) =>
        // Parameters always comes as AnyValue
        expressions.Parameter(name, context.namer.newVarName(), CypherCodeGenType(cypherType, AnyValueType))

      case lit: ast.IntegerLiteral => Literal(lit.value)

      case lit: ast.DoubleLiteral => Literal(lit.value)

      case lit: ast.StringLiteral => Literal(lit.value)

      case lit: ast.Literal => Literal(lit.value)

      case ast.ListLiteral(exprs) =>
        expressions.ListLiteral(exprs.map(e => callback(e)))

      case ast.Add(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Addition(leftOp, rightOp)

      case ast.Subtract(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Subtraction(leftOp, rightOp)

      case ast.Multiply(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Multiplication(leftOp, rightOp)

      case ast.Divide(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Division(leftOp, rightOp)

      case ast.Modulo(lhs, rhs) =>
        val leftOp = callback(lhs)
        val rightOp = callback(rhs)
        Modulo(leftOp, rightOp)

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

      case ast.Variable(name) => LoadVariable(context.getVariable(name))

      case other => throw new CantCompileQueryException(s"Expression of $other not yet supported")
    }
  }

}
