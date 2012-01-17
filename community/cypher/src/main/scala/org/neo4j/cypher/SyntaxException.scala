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
package org.neo4j.cypher

import org.neo4j.helpers.ThisShouldNotHappenError

class SyntaxException(message: String, val offset: Option[Int]=None) extends CypherException(message, null) {

  override def toString(query: String) : String = offset match {
    case Some(value) => getMessage + "\n" + findErrorLine(value, query.split('\n'))
    case None => getMessage
  }

  private def findErrorLine(offset: Int, message: Seq[String]): String =
    message.toList match {
      case Nil => throw new ThisShouldNotHappenError("Andrés & Tobias", "message converted to empty list")
      case head :: tail => {
        if (head.size > offset) {
          "\"" + head + "\"\n" + " " * offset + " ^"
        } else {
          findErrorLine(offset - head.size - 1, tail) //The extra minus one is there for the now missing \n
        }
      }
    }
}