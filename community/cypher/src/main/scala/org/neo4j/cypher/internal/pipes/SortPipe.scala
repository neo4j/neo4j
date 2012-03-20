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
package org.neo4j.cypher.internal.pipes

import scala.math.signum
import org.neo4j.cypher.internal.commands.SortItem
import java.lang.String
import org.neo4j.cypher.internal.Comparer
import collection.mutable.Map

class SortPipe(source: Pipe, sortDescription: List[SortItem]) extends Pipe with Comparer {
  val symbols = source.symbols

  assertDependenciesAreMet()

  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = source.createResults(params).toList.sortWith((a, b) => compareBy(a, b, sortDescription))

  def compareBy(a: Map[String, Any], b: Map[String, Any], order: Seq[SortItem]): Boolean = order match {
    case Nil => false
    case head :: tail => {
      val key = head.expression.identifier.name
      val aVal = a(key)
      val bVal = b(key)
      signum(compare(aVal, bVal)) match {
        case 1 => !head.ascending
        case -1 => head.ascending
        case 0 => compareBy(a, b, tail)
      }
    }
  }

  override def executionPlan(): String = source.executionPlan() + "\r\nSort(" + sortDescription.mkString(",") + ")"

  private def assertDependenciesAreMet() {
    sortDescription.map(_.expression.identifier).foreach( source.symbols.assertHas )
  }
}