/**
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
package org.neo4j.cypher.internal.compiler.v2_0.data

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.expressions.Expression
import org.neo4j.cypher.internal.helpers._
import scala.collection.JavaConverters._

// Values we are willing to expose as arguments in Java-side PlanDescriptions
//
// NOTE: Please update PlanDescription javadoc whenever you change the set of valid SimpleVals
// NOTE: Roughly maps to the JSON data model; don't build graph-shaped data structures with these, only trees please for
// REST interoperability
sealed abstract class SimpleVal extends StringRenderingSupport {
  type Value
  type JValue

  val v: Value

  def asJava: JValue

  override def render(builder: StringBuilder) {
    builder ++= v.toString
  }
}

final case class PrimVal[T <: AnyVal](v: T) extends SimpleVal {
  override type Value = T
  override type JValue = T

  override def asJava: JValue = v
}

final case class StrVal(v: String) extends SimpleVal {
  override type Value = String
  override type JValue = String

  override def asJava: JValue = v

  override def render(builder: StringBuilder) {
    builder += '"'
    builder.append(SimpleVal.escapeString(v))
    builder += '"'
  }
}

final case class MapVal(v: Map[String, SimpleVal]) extends SimpleVal {
  override type Value = Map[String, SimpleVal]
  override type JValue = java.util.Map[String, Any]

  override def asJava: JValue =
    Materialized.mapValues(v, (v: SimpleVal) => v.asJava.asInstanceOf[Any]).asJava

  override def render(builder: StringBuilder) {
    render(builder, "{", "}", "{}", escKey = true)
  }

  def render(builder: StringBuilder, startParen: String, stopParen: String, emptyParen: String, escKey: Boolean) {
    if (! v.isEmpty) {
      builder ++= startParen
      val pairs = v.toSeq
      renderPair(builder, pairs.head, escKey)
      for (pair <- pairs.tail) {
        builder ++= ", "
        renderPair(builder, pair, escKey)
      }
      builder ++= stopParen
    }
    else {
      builder ++= emptyParen
    }
  }

  private def renderPair(builder: StringBuilder, pair: (String, SimpleVal), escKey: Boolean) {
    val (key, value) = pair
    if (escKey) {
      builder += '"'
      builder ++= SimpleVal.escapeString(key)
      builder ++= "\": "
      value.render(builder)
    } else {
      builder ++= SimpleVal.escapeString(key)
      builder ++= "="
      value.render(builder)
    }
  }
}

final case class SeqVal(v: Seq[SimpleVal]) extends SimpleVal {
  override type Value = Seq[SimpleVal]
  override type JValue = java.lang.Iterable[Any]

  override def asJava: JValue = v.map(_.asJava.asInstanceOf[Any]).toIterable.asJava

  override def render(builder: StringBuilder) {
    builder += '['
    if (! v.isEmpty) {
      v.head.render(builder)
      for (value <- v.tail) {
        builder.append(", ")
        value.render(builder)
      }
    }
    builder += ']'
  }
}

object SimpleVal {
  def escapeString(s: String): String = s.replace("\"", "\\\"")

  implicit def fromStr[T](v: T): StrVal = StrVal(v.toString)

  implicit def fromMap[V](v: Map[String, SimpleVal]): MapVal = MapVal(v)

  implicit def fromIterable[V](v: Iterable[V], conv: V => SimpleVal): SeqVal = SeqVal(v.map(conv).toSeq)

  implicit def fromIterable[V](v: Iterable[V]): SeqVal = fromIterable(v, fromStr)

  implicit def fromSeq[T](values: Seq[T]): SeqVal = SeqVal(values.map(v => SimpleVal.fromStr(v)))

  implicit def fromExpr(e: Expression) = StrVal(e.toString())
}

