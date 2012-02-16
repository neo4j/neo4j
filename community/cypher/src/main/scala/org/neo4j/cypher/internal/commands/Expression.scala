/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

import scala.collection.JavaConverters._
import java.lang.String
import org.neo4j.cypher._
import internal.symbols._
import org.neo4j.graphdb.{Path, Relationship, NotFoundException, PropertyContainer, Node}
import collection.Seq

abstract class Expression extends (Map[String, Any] => Any) {
  def identifier: Identifier

  def declareDependencies(extectedType: AnyType): Seq[Identifier]

  def dependencies(extectedType: AnyType): Seq[Identifier] = {
    val myType = identifier.typ
    if (!extectedType.isAssignableFrom(myType))
      throw new SyntaxException(identifier.name + " expected to be of type " + extectedType + " but it is of type " + identifier.typ)
    declareDependencies(extectedType)
  }

  def rewrite(f: Expression => Expression): Expression

  def exists(f: Expression => Boolean): Boolean
}

case class Add(a: Expression, b: Expression) extends Expression {
  def identifier = Identifier(a.identifier.name + " + " + b.identifier.name, ScalarType())

  def apply(m: Map[String, Any]) = {
    val aVal = a(m)
    val bVal = b(m)

    (aVal, bVal) match {
      case (x: Number, y: Number) => x.doubleValue() + y.doubleValue()
      case (x: String, y: String) => x + y
      case _ => throw new CypherTypeException("Don't know how to add `" + aVal.toString + "` and `" + bVal.toString + "`")
    }

  }

  def declareDependencies(extectedType: AnyType) = a.declareDependencies(extectedType) ++ b.declareDependencies(extectedType)

  def rewrite(f: (Expression) => Expression) = f(Add(a.rewrite(f), b.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || a.exists(f) || b.exists(f)
}

case class Subtract(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "-"

  def verb = "subtract"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() - b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Modulo(a.rewrite(f), b.rewrite(f)))
}

case class Modulo(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "%"

  def verb = "modulo"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() % b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Modulo(a.rewrite(f), b.rewrite(f)))
}

case class Pow(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "^"

  def verb = "power"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = math.pow(a.doubleValue(), b.doubleValue())

  def rewrite(f: (Expression) => Expression) = f(Pow(a.rewrite(f), b.rewrite(f)))
}

case class Multiply(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "*"

  def verb = "multiply"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() * b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Multiply(a.rewrite(f), b.rewrite(f)))
}

case class Divide(a: Expression, b: Expression) extends Arithmetics(a, b) {
  def operand = "/"

  def verb = "divide"

  def stringWithString(a: String, b: String) = throwTypeError(a, b)

  def numberWithNumber(a: Number, b: Number) = a.doubleValue() / b.doubleValue()

  def rewrite(f: (Expression) => Expression) = f(Divide(a.rewrite(f), b.rewrite(f)))
}

abstract class Arithmetics(left: Expression, right: Expression) extends Expression {
  def identifier = Identifier("%s %s %s".format(left.identifier.name, operand, right.identifier.name), ScalarType())

  def operand: String

  def throwTypeError(bVal: Any, aVal: Any): Nothing = {
    throw new CypherTypeException("Don't know how to subtract `" + bVal.toString + "` from `" + aVal.toString + "`")
  }

  def apply(m: Map[String, Any]) = {
    val aVal = left(m)
    val bVal = right(m)

    (aVal, bVal) match {
      case (x: Number, y: Number) => numberWithNumber(x, y)
      case (x: String, y: String) => stringWithString(x, y)
      case _ => throwTypeError(bVal, aVal)
    }

  }

  def verb: String

  def stringWithString(a: String, b: String): String

  def numberWithNumber(a: Number, b: Number): Number

  def declareDependencies(extectedType: AnyType) = left.declareDependencies(extectedType) ++ right.declareDependencies(extectedType)

  def exists(f: (Expression) => Boolean) = f(this) || left.exists(f) || right.exists(f)
}

case class Literal(v: Any) extends Expression {
  def apply(m: Map[String, Any]) = v

  def identifier = Identifier(v.toString, AnyType.fromJava(v))

  override def toString() = if (v.isInstanceOf[String]) "\"" + v + "\"" else v.toString

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq()

  def rewrite(f: (Expression) => Expression) = f(this)

  def exists(f: (Expression) => Boolean) = f(this)
}

abstract class CastableExpression extends Expression {
  override def dependencies(extectedType: AnyType): Seq[Identifier] = declareDependencies(extectedType)
}

case class Nullable(expression: Expression) extends Expression {
  def identifier = Identifier(expression.identifier.name + "?", expression.identifier.typ)

  def apply(m: Map[String, Any]) = try {
    expression.apply(m)
  } catch {
    case x: EntityNotFoundException => null
  }

  def declareDependencies(extectedType: AnyType) = expression.dependencies(extectedType)

  override def dependencies(extectedType: AnyType) = expression.dependencies(extectedType)

  def rewrite(f: (Expression) => Expression) = f(Nullable(expression.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || expression.exists(f)

  override def toString() = expression.toString() + "?"
}

case class Property(entity: String, property: String) extends CastableExpression {
  def apply(m: Map[String, Any]): Any = {
    m(entity).asInstanceOf[PropertyContainer] match {
      case null => null
      case propertyContainer => try {
        propertyContainer.getProperty(property)
      } catch {
        case x: NotFoundException => throw new EntityNotFoundException("The property '%s' does not exist on %s".format(property, propertyContainer), x)
      }
    }
  }

  def identifier: Identifier = Identifier(entity + "." + property, ScalarType())

  override def toString(): String = entity + "." + property

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq(Identifier(entity, MapType()))

  def rewrite(f: (Expression) => Expression) = f(this)

  def exists(f: (Expression) => Boolean) = f(this)
}



case class Entity(entityName: String) extends CastableExpression {
  def apply(m: Map[String, Any]): Any = m.getOrElse(entityName, throw new NotFoundException)

  def identifier: Identifier = Identifier(entityName, AnyType())

  override def toString(): String = entityName

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq(Identifier(entityName, extectedType))

  def rewrite(f: (Expression) => Expression) = f(this)

  def exists(f: (Expression) => Boolean) = f(this)
}

case class Parameter(parameterName: String) extends CastableExpression {
  def apply(m: Map[String, Any]): Any = m.getOrElse("-=PARAMETER=-" + parameterName + "-=PARAMETER=-", throw new ParameterNotFoundException("Expected a parameter named " + parameterName)) match {
    case ParameterValue(x) => x
    case _ => throw new ParameterNotFoundException("Expected a parameter named " + parameterName)
  }

  def identifier: Identifier = Identifier(parameterName, AnyType())

  override def toString(): String = "{" + parameterName + "}"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq()

  def rewrite(f: (Expression) => Expression) = f(this)

  def exists(f: (Expression) => Boolean) = f(this)
}

case class ParameterValue(value: Any)