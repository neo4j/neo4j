/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher._
import internal.commands.AstNode
import internal.ExecutionContext
import internal.helpers.TypeSafeMathSupport
import internal.pipes.QueryState
import internal.symbols._

abstract class Expression extends Typed with TypeSafe with AstNode[Expression] {
  def rewrite(f: Expression => Expression): Expression

  def subExpressions = filter( _ != this)

  def containsAggregate = exists(_.isInstanceOf[AggregationExpression])

  def apply(ctx: ExecutionContext)(implicit state: QueryState):Any

  /*When calculating the type of an expression, the expression should also
  make sure to check the types of any downstream expressions*/
  protected def calculateType(symbols: SymbolTable): CypherType

  def evaluateType(expectedType: CypherType, symbols: SymbolTable): CypherType = {
    val t = calculateType(symbols)

    if (!expectedType.isAssignableFrom(t) &&
        !t.isAssignableFrom(expectedType)) {
      throw new CypherTypeException("%s expected to be of type %s but it is of type %s".format(this, expectedType, t))
    }

    t
  }

  protected def calculateUpperTypeBound(expectedType: CypherType, symbols: SymbolTable, exprs: Seq[Expression]): CypherType =
    exprs.map(_.evaluateType(expectedType, symbols)).reduce(_ mergeWith _)

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    evaluateType(AnyType(), symbols)
  }

  override def toString() = getClass.getSimpleName
}

case class CachedExpression(key:String, typ:CypherType) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ctx(key)

  def rewrite(f: (Expression) => Expression) = f(this)

  def children = Seq()

  def calculateType(symbols: SymbolTable) = typ

  def symbolTableDependencies = Set(key)

  override def toString() = "Cached(%s of type %s)".format(key, typ)
}

abstract class Arithmetics(left: Expression, right: Expression)
  extends Expression with TypeSafeMathSupport {
  def throwTypeError(bVal: Any, aVal: Any): Nothing = {
    throw new CypherTypeException("Don't know how to " + this + " `" + bVal + "` with `" + aVal + "`")
  }

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = {
    val aVal = left(ctx)
    val bVal = right(ctx)

    (aVal, bVal) match {
      case (x: Number, y: Number) => calc(x, y)
      case _ => throwTypeError(bVal, aVal)
    }
  }

  def calc(a: Number, b: Number): Any

  def calculateType(symbols: SymbolTable): CypherType = {
    left.evaluateType(NumberType(), symbols)
    right.evaluateType(NumberType(), symbols)
    NumberType()
  }

  def children = Seq(left, right)
}

trait ExpressionWInnerExpression extends Expression {
  def inner:Expression
  def myType:CypherType
  def expectedInnerType:CypherType

  def calculateType(symbols: SymbolTable): CypherType = {
    inner.evaluateType(expectedInnerType, symbols)

    myType
  }
}