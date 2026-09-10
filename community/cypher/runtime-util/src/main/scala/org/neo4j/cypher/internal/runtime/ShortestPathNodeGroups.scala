/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualValues

/**
 * P10 deterministic node-group reconstruction for directed single-relationship QPPs.
 *
 * For a returned path p = (v0,e1,v1,...,ek,vk) each traversed edge contributes exactly one
 * left-node and one right-node binding, so:
 *   left  = nodes(p)[0 .. k-1] (prefix, empty when k=0)
 *   right = nodes(p)[1 .. k]   (suffix, empty when k=0)
 *
 * Pure O(k) output materialization; traversal itself is unchanged ordinary shortest-path BFS.
 */
object ShortestPathNodeGroups {

  def prefixNodes(path: VirtualPathValue): org.neo4j.values.virtual.ListValue = {
    val nodeIds = path.nodeIds()
    val nEdges = path.size()
    if (nEdges == 0) VirtualValues.EMPTY_LIST
    else {
      val b = ListValueBuilder.newListBuilder(nodeIds.length - 1)
      var i = 0
      while (i < nEdges && i < nodeIds.length - 1) {
        b.add(VirtualValues.node(nodeIds(i)))
        i += 1
      }
      b.build()
    }
  }

  def suffixNodes(path: VirtualPathValue): org.neo4j.values.virtual.ListValue = {
    val nodeIds = path.nodeIds()
    val nEdges = path.size()
    if (nEdges == 0) VirtualValues.EMPTY_LIST
    else {
      val b = ListValueBuilder.newListBuilder(nodeIds.length - 1)
      var i = 1
      while (i < nodeIds.length) {
        b.add(VirtualValues.node(nodeIds(i)))
        i += 1
      }
      b.build()
    }
  }
}
