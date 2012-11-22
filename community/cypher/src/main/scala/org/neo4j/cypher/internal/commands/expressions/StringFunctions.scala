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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.CypherTypeException
import scala.collection.JavaConverters._
import collection.Map
import org.neo4j.cypher.internal.helpers.{IsCollection, CollectionSupport}
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.StringExtras
import org.neo4j.cypher.internal.pipes.ExecutionContext
import org.neo4j.cypher.internal.spi.QueryContext

abstract class StringFunction(arg: Expression) extends NullInNullOutExpression(arg) with StringHelper with CollectionSupport {
  def innerExpectedType = StringType()

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ arg.filter(f)
  else
    arg.filter(f)

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = arg.symbolTableDependencies
}

trait StringHelper {
  protected def asString(a: Any): String = a match {
    case null      => null
    case x: String => x
    case _         => throw new CypherTypeException("Expected a string value for %s, but got: %s; perhaps you'd like to cast to a string it with str().".format(toString, a.toString))
  }

  protected def props(x: PropertyContainer, q: QueryContext): String = {

    val keyValStrings = x match {
      case n: Node         => q.nodeOps().propertyKeys(n).asScala.map(key => key + ":" + text(q.nodeOps().getProperty(n, key), q))
      case r: Relationship => q.relationshipOps().propertyKeys(r).asScala.map(key => key + ":" + text(q.relationshipOps().getProperty(r, key), q))
    }

    keyValStrings.mkString("{", ",", "}")
  }

  protected def text(a: Any, ctx: QueryContext): String = a match {
    case x: Node            => x.toString + props(x, ctx)
    case x: Relationship    => ":" + x.getType.toString + "[" + x.getId + "] " + props(x, ctx)
    case IsCollection(coll) => coll.map(elem => text(elem, ctx)).mkString("[", ",", "]")
    case x: String          => "\"" + x + "\""
    case Some(x)            => x.toString
    case null               => "<null>"
    case x                  => x.toString
  }
}

case class StrFunction(argument: Expression) extends StringFunction(argument) with StringHelper with StringExtras {
  def compute(value: Any, m: ExecutionContext): Any = text(argument(m), m.state.query)

  def rewrite(f: (Expression) => Expression) = f(StrFunction(argument.rewrite(f)))
}

case class LowerFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext): Any = asString(argument(m)).toLowerCase

  def rewrite(f: (Expression) => Expression) = f(LowerFunction(argument.rewrite(f)))
}

case class UpperFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext): Any = asString(argument(m)).toUpperCase

  def rewrite(f: (Expression) => Expression) = f(UpperFunction(argument.rewrite(f)))
}

case class LTrimFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext): Any = asString(argument(m)).replaceAll("^\\s+", "")

  def rewrite(f: (Expression) => Expression) = f(LTrimFunction(argument.rewrite(f)))
}

case class RTrimFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext): Any = asString(argument(m)).replaceAll("\\s+$", "")

  def rewrite(f: (Expression) => Expression) = f(RTrimFunction(argument.rewrite(f)))
}

case class TrimFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext): Any = asString(argument(m)).trim

  def rewrite(f: (Expression) => Expression) = f(TrimFunction(argument.rewrite(f)))
}

case class SubstringFunction(orig: Expression, start: Expression, length: Expression) extends NullInNullOutExpression(orig) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext): Any = {
    val origVal = asString(orig(m))
    // if start goes off the end of the string, let's be nice and handle that.
    val startVal = if (origVal.length < asInt(start(m))) origVal.length
    else asInt(start(m))
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m)) + startVal) origVal.length - startVal
    else asInt(length(m))
    origVal.substring(startVal, startVal + lengthVal)
  }

  def filter(f: (Expression) => Boolean) = {
    val inner = orig.filter(f) ++ start.filter(f) ++ length.filter(f)
    if (f(this)) {
      Seq(this) ++ inner
    }
    else {
      inner
    }
  }

  def rewrite(f: (Expression) => Expression) = f(SubstringFunction(orig.rewrite(f), start.rewrite(f), length.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
    start.symbolTableDependencies ++
    length.symbolTableDependencies
}

case class ReplaceFunction(orig: Expression, search: Expression, replaceWith: Expression) extends NullInNullOutExpression(orig) with StringHelper {
  def compute(value: Any, m: ExecutionContext): Any = {
    val origVal = asString(value)
    val searchVal = asString(search(m))
    val replaceWithVal = asString(replaceWith(m))

    if (searchVal == null || replaceWithVal == null) {
      null
    } else {
      origVal.replace(searchVal, replaceWithVal)
    }
  }

  def filter(f: (Expression) => Boolean) = {
    val inner = orig.filter(f) ++ search.filter(f) ++ replaceWith.filter(f)
    if (f(this)) {
      Seq(this) ++ inner
    }
    else {
      inner
    }
  }

  def rewrite(f: (Expression) => Expression) = f(ReplaceFunction(orig.rewrite(f), search.rewrite(f), replaceWith.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
    search.symbolTableDependencies ++
    replaceWith.symbolTableDependencies
}

case class LeftFunction(orig: Expression, length: Expression) extends NullInNullOutExpression(orig) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext): Any = {
    val origVal = asString(orig(m))
    val startVal = asInt(0)
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m)) + startVal) origVal.length
    else asInt(length(m))
    origVal.substring(startVal, startVal + lengthVal)
  }

  def filter(f: (Expression) => Boolean) = {
    val inner = orig.filter(f) ++ length.filter(f)
    if (f(this)) {
      Seq(this) ++ inner
    }
    else {
      inner
    }
  }

  def rewrite(f: (Expression) => Expression) = f(LeftFunction(orig.rewrite(f), length.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}

case class RightFunction(orig: Expression, length: Expression) extends NullInNullOutExpression(orig) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext): Any = {
    val origVal = asString(orig(m))
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m))) origVal.length
    else asInt(length(m))
    val startVal = origVal.length - lengthVal
    origVal.substring(startVal, startVal + lengthVal)
  }

  def filter(f: (Expression) => Boolean) = {
    val inner = orig.filter(f) ++ length.filter(f)
    if (f(this)) {
      Seq(this) ++ inner
    }
    else {
      inner
    }
  }

  def rewrite(f: (Expression) => Expression) = f(RightFunction(orig.rewrite(f), length.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
    length.symbolTableDependencies
}
