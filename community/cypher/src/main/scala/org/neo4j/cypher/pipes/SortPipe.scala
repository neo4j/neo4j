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
package org.neo4j.cypher.pipes

import org.neo4j.cypher.{Comparer, SymbolTable}
import scala.math.signum
import org.neo4j.cypher.commands.SortItem

class SortPipe(source: Pipe,sortDescription: List[SortItem]) extends Pipe with Comparer {
  val symbols: SymbolTable = source.symbols

  def foreach[U](f: (Map[String, Any]) => U) {
    val sorted = source.toList.sortWith((a, b) => compareBy (a,b,sortDescription))

    sorted.foreach(f)
  }

  def compareBy(a:Map[String, Any], b:Map[String, Any], order:Seq[SortItem]):Boolean = order match {
    case Nil => false
    case head :: tail => {
      val key = head.returnItem.identifier.name
      val aVal = a(key)
      val bVal = b(key)
      signum(compare(aVal, bVal)) match {
        case 1 => !head.ascending
        case -1 => head.ascending
        case 0 => compareBy(a,b,tail)
      }
    }
  }
}