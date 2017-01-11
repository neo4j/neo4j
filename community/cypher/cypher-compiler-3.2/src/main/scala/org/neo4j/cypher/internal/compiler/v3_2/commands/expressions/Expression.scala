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
package org.neo4j.cypher.internal.compiler.v3_2.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.helpers.TypeSafeMathSupport
import org.neo4j.cypher.internal.compiler.v3_2.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_2.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_2.Foldable._
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CypherType

abstract class Expression {
  def containsAggregate = this.exists {
    case _: AggregationExpression => true
  }

  def apply(ctx: ExecutionContext)(implicit state: QueryState):Any

  override def toString = this match {
    case p: Product => scala.runtime.ScalaRunTime._toString(p)
    case _          => getClass.getSimpleName
  }

  def isDeterministic: Boolean = ! this.exists {
    case RandFunction() => true
    case _              => false
  }
}

case class CachedExpression(key:String, typ:CypherType) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ctx(key)

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
}

trait ExpressionWInnerExpression extends Expression {
  def inner:Expression
  def myType:CypherType
  def expectedInnerType:CypherType
}

object Expression {
  def mapExpressionHasPropertyReadDependency(mapEntityName: String, mapExpression: Expression): Boolean =
    mapExpression match {
      case LiteralMap(map) => map.exists {
        case (k, v) => v.exists {
          case Property(Variable(entityName), propertyKey) =>
            entityName == mapEntityName && propertyKey.name == k
        }
      }
    }

  def hasPropertyReadDependency(entityName: String, expression: Expression, propertyKey: String): Boolean =
    expression.exists {
      case Property(Variable(name), key) =>
        name == entityName && key.name == propertyKey
    }
}
