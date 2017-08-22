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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.AstNode
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.TypeSafeMathSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.Pipe
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.symbols.TypeSafe
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

abstract class Expression extends TypeSafe with AstNode[Expression] {

  // WARNING: MUTABILITY IN IMMUTABLE CLASSES ...
  private var _owningPipe: Option[Pipe] = None

  def owningPipe: Pipe = _owningPipe.get

  def registerOwningPipe(pipe: Pipe): Unit = visit {
    case x: Expression => x._owningPipe = Some(pipe)
  }
  // ... TREAD WITH CAUTION

  def rewrite(f: Expression => Expression): Expression

  def rewriteAsPredicate(f: Expression => Expression): Predicate = rewrite(f) match {
    case pred: Predicate => pred
    case e               => CoercedPredicate(e)
  }

  def subExpressions: Seq[Expression] = {
    def expandAll(e: AstNode[_]): Seq[AstNode[_]] = e.children ++ e.children.flatMap(expandAll)
    expandAll(this).collect {
      case e: Expression => e
    }
  }

  // Expressions that do not get anything in their context from this expression.
  def arguments: Seq[Expression]

  // Any expressions that this expression builds on
  def children: Seq[AstNode[_]] = arguments

  def containsAggregate = exists(_.isInstanceOf[AggregationExpression])

  def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue

  override def toString = this match {
    case p: Product => scala.runtime.ScalaRunTime._toString(p)
    case _          => getClass.getSimpleName
  }

  val isDeterministic = !exists {
    case RandFunction() => true
    case _              => false
  }
}

case class CachedExpression(key: String, typ: CypherType) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ctx(key)

  def rewrite(f: (Expression) => Expression) = f(this)

  def arguments = Seq()

  def symbolTableDependencies = Set(key)

  override def toString = "Cached(%s of type %s)".format(key, typ)
}

abstract class Arithmetics(left: Expression, right: Expression) extends Expression with TypeSafeMathSupport {
  def throwTypeError(bVal: Any, aVal: Any): Nothing = {
    throw new CypherTypeException("Don't know how to " + this + " `" + bVal + "` with `" + aVal + "`")
  }

  def apply(ctx: ExecutionContext)(implicit state: QueryState): AnyValue = {
    val aVal = left(ctx)
    val bVal = right(ctx)

    (aVal, bVal) match {
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
      case (x: NumberValue, y: NumberValue)                       => calc(x, y)
      case _                                                      => throwTypeError(bVal, aVal)
    }
  }

  def calc(a: NumberValue, b: NumberValue): AnyValue

  def arguments = Seq(left, right)
}

trait ExpressionWInnerExpression extends Expression {
  def inner: Expression
  def myType: CypherType
  def expectedInnerType: CypherType
}

object Expression {
  def mapExpressionHasPropertyReadDependency(mapEntityName: String, mapExpression: Expression): Boolean =
    mapExpression match {
      case LiteralMap(map) =>
        map.exists {
          case (k, v) =>
            v.subExpressions.exists {
              case Property(Variable(entityName), propertyKey) =>
                entityName == mapEntityName && propertyKey.name == k
              case _ => false
            }
        }
      case _ => false
    }

  def hasPropertyReadDependency(entityName: String, expression: Expression, propertyKey: String): Boolean =
    expression.subExpressions.exists {
      case Property(Variable(name), key) =>
        name == entityName && key.name == propertyKey
      case _ =>
        false
    }
}
