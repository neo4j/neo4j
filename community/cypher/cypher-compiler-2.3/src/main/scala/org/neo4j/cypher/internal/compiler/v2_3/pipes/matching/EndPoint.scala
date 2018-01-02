/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.PropertyContainer

final case class EndPoint(name: String) extends Trail {
  val end = name

  def pathDescription = nodeNames

  val start = name

  val isEndPoint = true

  val size = 0

  def toSteps(id: Int) = None

  protected[matching] def decompose(p: Seq[PropertyContainer], mapSoFar: Map[String, Any]) =
    if (!p.isEmpty && p.tail.isEmpty) {
      val existingValue = mapSoFar.get(name)
      val endNode = p.head

      existingValue match {
        case Some(existing) if endNode != existing => Iterator.empty
        case _                                     => Iterator.single((Nil, mapSoFar + (name -> endNode)))
      }
    } else {
      Iterator.empty
    }

  def symbols(table: SymbolTable): SymbolTable = table.add(name, CTNode)

  def contains(target: String): Boolean = target == name

  def predicates = Nil

  def patterns = Nil

  override def toString = "(" + name + ")"

  val nodeNames = Seq(name)

  def add(f: (String) => Trail) = f(name)

  def filter(f: (Trail) => Boolean):Iterable[Trail] = Some(this).filter(f)
}
