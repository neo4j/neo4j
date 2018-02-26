/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.queryReduction.ast

import org.neo4j.cypher.internal.util.v3_4._

object ASTNodeHelper {

  def getDepth(node: ASTNode): Int = {
    val children = getChildren(node)
    if (children.isEmpty) {
      0
    } else {
      children.map(getDepth).reduce(Math.max) + 1
    }
  }

  def countNodesOnLevel(node: ASTNode, level: Int): Int = {
    val children = getChildren(node)
    if (level == 0) {
      1
    } else {
      children.map(countNodesOnLevel(_, level - 1)).sum
    }
  }

  def getSize(node: ASTNode): Int = {
    getChildren(node).map(getSize).fold(1)(_ + _)
  }

  def forallNodes(node: ASTNode)(predicate: ASTNode => Boolean): Boolean = {
    getChildren(node).map(forallNodes(_)(predicate)).fold(predicate(node))(_ && _)
  }

  def existsNode(node: ASTNode)(predicate: ASTNode => Boolean): Boolean = {
    getChildren(node).map(existsNode(_)(predicate)).fold(predicate(node))(_ || _)
  }

}
