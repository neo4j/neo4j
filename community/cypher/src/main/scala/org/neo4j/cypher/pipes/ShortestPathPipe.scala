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

import org.neo4j.graphalgo.GraphAlgoFactory
import org.neo4j.kernel.Traversal
import org.neo4j.graphdb.{DynamicRelationshipType, Direction, Node}
import org.neo4j.cypher.commands.{ShortestPath, PathIdentifier}
import org.neo4j.cypher.{SyntaxException, SymbolTable}

class ShortestPathPipe(source: Pipe, pipeName: String, startName: String, endName: String, relType:Option[String], dir:Direction, maxDepth:Option[Int], optional: Boolean) extends Pipe {

  def this(source:Pipe, ast:ShortestPath) = this(source, ast.pipeName, ast.startName, ast.endName, ast.relType, ast.dir, ast.maxDepth, ast.optional)

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(m => {
      val err = (n:String) => throw new SyntaxException("Shortest path needs both ends of the path to be provided. Couldn't find " + n)

      val start = m.getOrElse(startName, err(startName)).asInstanceOf[Node]
      val end = m.getOrElse(endName, err(endName)).asInstanceOf[Node]

      val expander = relType match {
        case None => Traversal.expanderForAllTypes(dir)
        case Some(typeName) => Traversal.expanderForTypes(DynamicRelationshipType.withName(typeName), dir)
      }

      val finder = GraphAlgoFactory.shortestPath(expander, maxDepth.head)
      val findSinglePath = finder.findSinglePath(start, end)

      (findSinglePath, optional) match {
        case (null, true) => f(m ++ Map(pipeName -> null))
        case (null, false) =>
        case (path, _) => f(m ++ Map(pipeName -> path))
      }
    })
  }

  val symbols: SymbolTable = source.symbols.add(PathIdentifier(pipeName))
}

// My daughters wrote this when I left the laptop open 2011-09-22. Now it belongs here.
//      lola
//      nina
//      wilma
//      mimi

//      nina
//      andres tykör alla om andres är snel han jör mango till sina barn han tröstar sina barn när dom är lesna han -
//      jör god mat han läsör bok och han har jet mej en ajpäd
//      lola
//      jag älskar andres andres pappa är rar du är fin du har fint namn

