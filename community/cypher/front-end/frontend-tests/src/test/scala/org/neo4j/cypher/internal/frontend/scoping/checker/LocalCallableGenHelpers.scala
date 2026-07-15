/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.scoping.checker

import scala.util.Random

trait LocalCallableGenHelpers {

  def indent(x: String, y: String): String = {
    x.linesWithSeparators.map(line => y + line).mkString
  }

  sealed trait Kind {
    def keyword: String
  }

  case object Procedure extends Kind {
    override val keyword = "PROCEDURE"
  }

  case object Function extends Kind {
    override val keyword = "FUNCTION"
  }

  sealed trait Body {
    def cypher: String

    def renderFor(kind: Kind): String
  }

  case class Query(cypher: String) extends Body {

    def renderFor(kind: Kind): String = kind match {
      case Procedure =>
        s"""{
           |${indent(cypher, "  ")}
           |}""".stripMargin
      case Function =>
        s"""= head(COLLECT {
           |${indent(cypher, "  ")}
           |})""".stripMargin
    }
  }

  case class Expression(cypher: String, alias: String) extends Body {

    def renderFor(kind: Kind): String = kind match {
      case Procedure =>
        s"""{
           |  RETURN $cypher AS $alias
           |}""".stripMargin
      case Function =>
        s"""= $cypher""".stripMargin
    }
  }

  def variableNames(num: Int, offset: Int = 0): Seq[Char] = ('a' to 'z').slice(offset, offset + num)

  def pickOne[T](from: Seq[T])(implicit rand: Random): T = {
    if (from.isEmpty) {
      throw new RuntimeException(s"Cannot pick one item from a empty list of items.")
    } else {
      val index =
        if (from.size == 1) 0
        else rand.nextInt(from.size)
      from(index)
    }
  }

  def pickDistinct[T](from: Seq[T], num: Int)(implicit rand: Random): (Seq[T], Seq[T]) = {
    if (num > from.size) {
      throw new RuntimeException(s"Cannot pick $num distinct items from a list of ${from.size} items.")
    } else {
      if (num == 0) {
        (Seq.empty, from)
      } else {
        val index =
          if (from.size <= 1) 0
          else rand.nextInt(from.size)
        val pick = from(index)
        val pre = from.take(index)
        val post = from.drop(index + 1)
        val (picks, rest) = pickDistinct(pre ++ post, num - 1)
        (pick +: picks, rest)
      }
    }
  }
}
