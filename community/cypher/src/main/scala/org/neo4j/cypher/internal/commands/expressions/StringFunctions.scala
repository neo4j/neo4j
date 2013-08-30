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

import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.helpers.{IsMap, IsCollection, CollectionSupport}
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.{ExecutionContext}
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.cypher.internal.commands.values.KeyToken
import org.neo4j.cypher.internal.pipes.QueryState
import scala.collection.Map

abstract class StringFunction(arg: Expression) extends NullInNullOutExpression(arg) with StringHelper with CollectionSupport {
  def innerExpectedType = StringType()

  def children = Seq(arg)

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = arg.symbolTableDependencies
}

trait StringHelper {
  protected def asString(a: Any): String = a match {
    case null      => null
    case x: String => x
    case _         => throw new CypherTypeException("Expected a string value for %s, but got: %s; perhaps you'd like to cast to a string it with str().".format(toString, a.toString))
  }

  protected def props(x: PropertyContainer, qtx: QueryContext): String = {

    val keyValStrings = x match {
      case n: Node         => qtx.nodeOps.propertyKeyIds(n).map(pkId => qtx.getPropertyKeyName(pkId) + ":" + text(qtx.nodeOps.getProperty(n, pkId), qtx))
      case r: Relationship => qtx.relationshipOps.propertyKeyIds(r).map(pkId => qtx.getPropertyKeyName(pkId) + ":" + text(qtx.relationshipOps.getProperty(r, pkId), qtx))
    }

    keyValStrings.mkString("{", ",", "}")
  }

  protected def text(a: Any, qtx: QueryContext): String = a match {
    case x: Node            => x.toString + props(x, qtx)
    case x: Relationship    => ":" + x.getType.name() + "[" + x.getId + "]" + props(x, qtx)
    case IsMap(m)           => makeString(m, qtx)
    case IsCollection(coll) => coll.map(elem => text(elem, qtx)).mkString("[", ",", "]")
    case x: String          => "\"" + x + "\""
    case v: KeyToken        => v.name
    case Some(x)            => x.toString
    case null               => "<null>"
    case x                  => x.toString
  }

  private def makeString(m: QueryContext => Map[String, Any], qtx: QueryContext) = m(qtx).map {
    case (k, v) => k + " -> " + text(v, qtx)
  }.mkString("{", ", ", "}")

  def makeSize(txt: String, wantedSize: Int): String = {
    val actualSize = txt.length()
    if (actualSize > wantedSize) {
      txt.slice(0, wantedSize)
    } else if (actualSize < wantedSize) {
      txt + repeat(" ", wantedSize - actualSize)
    } else txt
  }

  def repeat(x: String, size: Int): String = (1 to size).map((i) => x).mkString
}

case class StrFunction(argument: Expression) extends StringFunction(argument) with StringHelper  {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = text(argument(m), state.query)

  def rewrite(f: (Expression) => Expression) = f(StrFunction(argument.rewrite(f)))
}

case class LowerFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).toLowerCase

  def rewrite(f: (Expression) => Expression) = f(LowerFunction(argument.rewrite(f)))
}

case class UpperFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).toUpperCase

  def rewrite(f: (Expression) => Expression) = f(UpperFunction(argument.rewrite(f)))
}

case class LTrimFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).replaceAll("^\\s+", "")

  def rewrite(f: (Expression) => Expression) = f(LTrimFunction(argument.rewrite(f)))
}

case class RTrimFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).replaceAll("\\s+$", "")

  def rewrite(f: (Expression) => Expression) = f(RTrimFunction(argument.rewrite(f)))
}

case class TrimFunction(argument: Expression) extends StringFunction(argument) with StringHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = asString(argument(m)).trim

  def rewrite(f: (Expression) => Expression) = f(TrimFunction(argument.rewrite(f)))
}

case class SubstringFunction(orig: Expression, start: Expression, length: Option[Expression])
  extends NullInNullOutExpression(orig) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))

    def noMoreThanMax(maxLength: Int, length: Int): Int =
      if (length > maxLength) {
        maxLength
      } else {
        length
      }

    // if start goes off the end of the string, let's be nice and handle that.
    val startVal = noMoreThanMax(origVal.length, asInt(start(m)))

    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = length match {
      case None       => origVal.length - startVal
      case Some(func) => noMoreThanMax(origVal.length - startVal, asInt(func(m)))
    }

    origVal.substring(startVal, startVal + lengthVal)
  }


  def children = Seq(orig, start) ++ length

  def rewrite(f: (Expression) => Expression) = f(SubstringFunction(orig.rewrite(f), start.rewrite(f), length.map(_.rewrite(f))))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = {
    val a = orig.symbolTableDependencies ++
            start.symbolTableDependencies

    val b = length.toSeq.flatMap(_.symbolTableDependencies.toSeq).toSet

    a ++ b
  }
}

case class ReplaceFunction(orig: Expression, search: Expression, replaceWith: Expression)
  extends NullInNullOutExpression(orig) with StringHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(value)
    val searchVal = asString(search(m))
    val replaceWithVal = asString(replaceWith(m))

    if (searchVal == null || replaceWithVal == null) {
      null
    } else {
      origVal.replace(searchVal, replaceWithVal)
    }
  }

  def children = Seq(orig, search, replaceWith)

  def rewrite(f: (Expression) => Expression) = f(ReplaceFunction(orig.rewrite(f), search.rewrite(f), replaceWith.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
                                search.symbolTableDependencies ++
                                replaceWith.symbolTableDependencies
}

case class LeftFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))
    val startVal = asInt(0)
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m)) + startVal) origVal.length
    else asInt(length(m))
    origVal.substring(startVal, startVal + lengthVal)
  }

  def children = Seq(orig, length)

  def rewrite(f: (Expression) => Expression) = f(LeftFunction(orig.rewrite(f), length.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
                                length.symbolTableDependencies
}

case class RightFunction(orig: Expression, length: Expression)
  extends NullInNullOutExpression(orig) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val origVal = asString(orig(m))
    // if length goes off the end of the string, let's be nice and handle that.
    val lengthVal = if (origVal.length < asInt(length(m))) origVal.length
    else asInt(length(m))
    val startVal = origVal.length - lengthVal
    origVal.substring(startVal, startVal + lengthVal)
  }

  def children = Seq(orig, length)

  def rewrite(f: (Expression) => Expression) = f(RightFunction(orig.rewrite(f), length.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = orig.symbolTableDependencies ++
                                length.symbolTableDependencies
}
