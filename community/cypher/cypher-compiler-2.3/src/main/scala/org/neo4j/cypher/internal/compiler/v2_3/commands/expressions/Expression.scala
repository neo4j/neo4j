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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{CoercedPredicate, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.helpers.TypeSafeMathSupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.{SymbolTable, TypeSafe, Typed}
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols.{CypherType, _}

abstract class Expression extends Typed with TypeSafe with EffectfulAstNode[Expression] {
  def rewrite(f: Expression => Expression): Expression

  def rewriteAsPredicate(f: Expression => Expression): Predicate = rewrite(f) match {
    case pred: Predicate => pred
    case e               => CoercedPredicate(e)
  }

  def subExpressions: Seq[Expression] = {
    def expandAll(e: AstNode[_]): Seq[AstNode[_]] = e.children ++ e.children.flatMap(expandAll)
    expandAll(this).collect {
      case e:Expression => e
    }
  }

  // Expressions that do not get anything in their context from this expression.
  def arguments:Seq[Expression]

  // Any expressions that this expression builds on
  def children: Seq[AstNode[_]] = arguments

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
    exprs.map(_.evaluateType(expectedType, symbols)).reduce(_ leastUpperBound _)

  override def toString = this match {
    case p: Product => scala.runtime.ScalaRunTime._toString(p)
    case _          => getClass.getSimpleName
  }

  def localEffects(symbols: SymbolTable) = Effects()

  val isDeterministic = ! exists {
    case RandFunction() => true
    case _              => false
  }
}

case class CachedExpression(key:String, typ:CypherType) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ctx(key)

  def rewrite(f: (Expression) => Expression) = f(this)

  def arguments = Seq()

  def calculateType(symbols: SymbolTable) = typ

  def symbolTableDependencies = Set(key)

  override def toString = "Cached(%s of type %s)".format(key, typ)
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
      case (null, _) => null
      case (_, null) => null
      case (x: Number, y: Number) => calc(x, y)
      case _ => throwTypeError(bVal, aVal)
    }
  }

  def calc(a: Number, b: Number): Any

  def calculateType(symbols: SymbolTable): CypherType = {
    left.evaluateType(CTNumber, symbols)
    right.evaluateType(CTNumber, symbols)
    CTNumber
  }

  def arguments = Seq(left, right)
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
