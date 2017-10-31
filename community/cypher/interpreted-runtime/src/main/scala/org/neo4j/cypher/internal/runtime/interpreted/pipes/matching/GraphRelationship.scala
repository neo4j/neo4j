/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes.matching

import org.neo4j.values.virtual._

abstract class GraphRelationship {
  def getOtherNode(node: NodeValue): NodeValue
}

case class SingleGraphRelationship(rel: EdgeValue) extends GraphRelationship {
  def getOtherNode(node: NodeValue): NodeValue = rel.otherNode(node)

  override def canEqual(that: Any) = that.isInstanceOf[SingleGraphRelationship] ||
    that.isInstanceOf[EdgeValue] ||
    that.isInstanceOf[VariableLengthGraphRelationship]

  override def equals(obj: Any) = obj match {
    case VariableLengthGraphRelationship(p) => p.edges().contains(rel)

    case p: PathValue => p.edges().contains(rel)
    case x => x == this || x == rel
  }

  override def toString = rel.toString
}

case class VariableLengthGraphRelationship(path: PathValue) extends GraphRelationship {
  def getOtherNode(node: NodeValue): NodeValue = {
    if (path.startNode() == node) path.endNode()
    else if (path.endNode() == node) path.startNode()
    else throw new IllegalArgumentException("Node is not start nor end of path.")
  }

  override def canEqual(that: Any) = that.isInstanceOf[VariableLengthGraphRelationship] ||
    that.isInstanceOf[PathValue] ||
    that.isInstanceOf[SingleGraphRelationship]

  override def equals(obj: Any) = obj match {
    case r: EdgeValue => path.edges().contains(r)
    case x => obj == this || (obj == path && path.size() > 0)
  }

  def relationships: ListValue = VirtualValues.list(path.edges():_*)

  override def toString = path.toString
}
