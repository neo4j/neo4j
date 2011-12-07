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

import org.neo4j.cypher.commands.Value
import collection.Seq
import java.lang.String
import org.neo4j.cypher.symbols.{PathType, NodeType, Identifier, SymbolTable}
import org.neo4j.graphdb._
import scala.collection.JavaConverters._
import org.neo4j.cypher.PathImpl


class AllLeafPathsPipe(source: Pipe,
                       start: Value,
                       endName: String,
                       pathName: String,
                       dir: Direction,
                       relType: Option[String],
                       maxLength: Option[Int])
  extends PipeWithSource(source) {

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(m => {
      val startNode = start(m).asInstanceOf[Node]
      follow(Seq(startNode), f, m)
    })
  }

  private def follow[U](path: Seq[PropertyContainer], f: (Map[String, Any]) => U, m: Map[String, Any]) {
    val endNode = path.last.asInstanceOf[Node]
    val rels = relType match {
      case None => endNode.getRelationships(dir).asScala
      case Some(typ) => endNode.getRelationships(dir, DynamicRelationshipType.withName(typ)).asScala
    }

    if (rels.isEmpty && path.size > 1) {
      f(m ++ Map(endName -> endNode, pathName -> PathImpl(path: _*)))
    } else {
      for (val rel: Relationship <- rels) {
        val next = rel.getOtherNode(endNode)
        follow(path ++ Seq(rel, next), f, m)
      }
    }
  }

  def dependencies: Seq[Identifier] = start.dependencies(NodeType())

  val symbols: SymbolTable = source.symbols.add(Identifier(endName, NodeType()), Identifier(pathName, PathType()))
}