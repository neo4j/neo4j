/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher

import java.lang.System.lineSeparator

import org.neo4j.kernel.api.exceptions.Status

class SyntaxException(message: String, val query:String,  val offset: Option[Int], cause: Throwable) extends CypherException(message, cause) {
  def this(message: String, query:String, offset: Option[Int]) = this(message,query,offset,null)
  def this(message: String, query:String, offset: Int) = this(message,query,Some(offset),null)
  def this(message:String, cause: Throwable) = this(message,"",None, cause)
  def this(message:String) = this(message,"",None,null)

  override def getMessage: String = offset match {
    case Some(idx) =>
      //split can be empty if query = '\n'
      val split = query.split("\r?\n").toList
      message + lineSeparator() + findErrorLine(idx, if (split.nonEmpty) split else List(""))
    case None => message
  }

  override val status = Status.Statement.SyntaxError

  private def findErrorLine(idx: Int, message: List[String]): String =
    message.toList match {
      case Nil => throw new IllegalArgumentException("message converted to empty list")

      case List(x) =>
        val spaces = if (x.length > idx)
          idx
        else
          x.length

        "\"" + x + "\"" + lineSeparator() +  " " * spaces + " ^"

      case head :: tail => if (head.length > idx) {
        "\"" + head + "\"\n" + " " * idx + " ^"
      } else {
        findErrorLine(idx - head.length - 1, tail) //The extra minus one is there for the now missing \n
      }
    }
}
