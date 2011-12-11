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
package org.neo4j.cypher.internal.pipes

import collection.Seq
import java.lang.String
import org.neo4j.cypher.symbols.{PathType, NodeType, Identifier, SymbolTable}
import org.neo4j.graphdb._
import org.neo4j.cypher.PathImpl
import org.neo4j.cypher.commands.AllLeafs
import scala.collection.JavaConverters._

class AllLeafPathsPipe(source: Pipe, leafInfo: AllLeafs) extends PipeWithSource(source) {
  def startName = leafInfo.startNode
  def relType = leafInfo.relType
  def dir = leafInfo.dir
  def endName = leafInfo.endName
  def pathName = leafInfo.pathName
  override def executionPlan(): String = source.executionPlan() + "\r\n" + "AllLeafPaths(" + leafInfo + ")"


  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = source.createResults(params).flatMap(m => {
    val startNode = m(startName).asInstanceOf[Node]
    follow(Seq(startNode), m)
  })

  private def follow[U](path: Seq[PropertyContainer], m: Map[String, Any]): Traversable[Map[String, Any]] = {
    val endNode = path.last.asInstanceOf[Node]
    val rels = relType match {
      case None => endNode.getRelationships(dir).asScala
      case Some(typ) => endNode.getRelationships(dir, DynamicRelationshipType.withName(typ)).asScala
    }

    if (rels.isEmpty && path.size > 1) {
      Seq(m ++ Map(endName -> endNode, pathName -> PathImpl(path: _*)))
    } else {
      rels.flatMap(rel => {
        val next = rel.getOtherNode(endNode)
        follow(path ++ Seq(rel, next), m)
      })
    }
  }

  def dependencies: Seq[Identifier] = Seq(Identifier(startName, NodeType()))

  val symbols: SymbolTable = source.symbols.add(Identifier(endName, NodeType()), Identifier(pathName, PathType()))
}