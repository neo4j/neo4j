/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.cypher.commands.{RelatedTo, PathItem, PathIdentifier}
import org.neo4j.cypher.{PathImpl, SymbolTable}
import org.neo4j.graphdb.PropertyContainer

class PathPipe(source: Pipe, path: PathItem) extends Pipe {
  def foreach[U](f: (Map[String, Any]) => U) {

    source.foreach(m => {
      def get(x:String):PropertyContainer = m(x).asInstanceOf[PropertyContainer]

      val firstNode = path.pathPattern.head match {
        case RelatedTo(left, right, relName, x, xx) => left
      }

      val p = Seq(get(firstNode)) ++ path.pathPattern.flatMap(p => p match {
        case RelatedTo(left, right, relName, x, xx) => Seq(get(relName), get(right))
      })

      val pathImpl = new PathImpl(p: _*)

      f( m + (path.pathName -> pathImpl) )
    })
  }

  val symbols: SymbolTable = source.symbols.add(Seq(PathIdentifier(path.pathName)))
}