/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.util

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition

object PrettyPrinting extends PrettyPrintingUtils

trait PrettyPrinting[T] extends PrettyPrintingUtils {
  def pretty: T => Stream[String]

  def node(name: String, fields: Seq[(String, Any)], children: Seq[T] = Seq()): Stream[String] = {
    def head(name: String) = Stream(s"[ $name ]")

    def middle(fields: Seq[(String, Any)]): Stream[String] = {
      val max = if (fields.nonEmpty) fields.map(_._1.length).max else 0

      fields.toStream.flatMap {
        case (name, vs: Stream[_]) =>
          val text = vs.map(e => "│ ┊ " + e.toString)
          Stream(s"╞ $name:") ++ text
        case (name, value) =>
          val space = " " * (max - name.length)
          Stream(s"╞ $name$space: $value")
      }
    }

    def rest(cs: Seq[T]) = cs match {
      case Seq() => Stream()
      case es =>
        es.init.toStream.flatMap(c => framing(pretty(c), "├─ ", "│    ")) ++
          framing(pretty(es.last), "└─ ", "     ")
    }

    def framing(in: Stream[String], first: String, rest: String): Stream[String] = {
      val head = first + in.head
      val tail = in.tail.map(rest + _)
      head #:: tail
    }

    head(name) ++ middle(fields) ++ rest(children)
  }

  def pprint(t: T): Unit =
    pretty(t).foreach(println)

  def asString(t: T): String =
    pretty(t).mkString(System.lineSeparator())
}

trait PrettyPrintingUtils {

  private val printer = Prettifier(ExpressionStringifier())

  def expr(e: Expression): String =
    printer.expr.apply(e)

  def query(s: Statement): Stream[String] =
    printer.asString(s).linesIterator.toStream

  def clause(c: Clause): String =
    query(Seq(c)).mkString("\n")

  def query(cs: Seq[Clause]): Stream[String] = {
    val pos = InputPosition.NONE
    query(SingleQuery(cs)(pos))
  }

  def list(ss: Seq[Any]): String =
    ss.mkString(",")
}
