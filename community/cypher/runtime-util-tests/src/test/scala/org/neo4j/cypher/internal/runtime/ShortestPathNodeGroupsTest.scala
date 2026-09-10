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

import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualPathValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

class ShortestPathNodeGroupsTest extends RuntimeUtilTestSuite {

  private def pathOf(nodeIds: Long*): VirtualPathValue = {
    val relIds = (0 until math.max(0, nodeIds.length - 1)).map(i => 100L + i).toArray
    VirtualValues.pathReference(nodeIds.toArray, relIds)
  }

  private def ids(v: org.neo4j.values.virtual.ListValue): Seq[Long] = {
    (0 until v.intSize()).map(i => v.value(i).asInstanceOf[VirtualNodeValue].id()).toSeq
  }

  test("two-edge path") {
    val p = pathOf(10, 20, 30)
    ids(ShortestPathNodeGroups.prefixNodes(p)) shouldEqual Seq(10, 20)
    ids(ShortestPathNodeGroups.suffixNodes(p)) shouldEqual Seq(20, 30)
  }

  test("zero-length path") {
    val p = pathOf(7)
    ShortestPathNodeGroups.prefixNodes(p).intSize() shouldEqual 0
    ShortestPathNodeGroups.suffixNodes(p).intSize() shouldEqual 0
  }

  test("one-edge path") {
    val p = pathOf(1, 2)
    ids(ShortestPathNodeGroups.prefixNodes(p)) shouldEqual Seq(1)
    ids(ShortestPathNodeGroups.suffixNodes(p)) shouldEqual Seq(2)
  }

  test("relationship list unaffected") {
    val p = pathOf(10, 20, 30)
    p.relationshipIds().length shouldEqual 2
    ids(ShortestPathNodeGroups.prefixNodes(p)).length shouldEqual 2
    ids(ShortestPathNodeGroups.suffixNodes(p)).length shouldEqual 2
  }
}
