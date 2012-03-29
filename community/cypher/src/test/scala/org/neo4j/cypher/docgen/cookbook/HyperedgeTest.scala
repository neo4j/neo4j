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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase
import org.neo4j.visualization.graphviz.{GraphStyle, AsciiDocSimpleStyle}

class HyperedgeTest extends DocumentingTestBase {
  def graphDescription = List("User1 in Group1", "User1 in Group2",
    "Group2 canHave Role2", "Group2 canHave Role1",
    "Group1 canHave Role1", "Group1 canHave Role2", "Group1 isA Group",
    "Group2 isA Group", "Role1 isA Role", "Role2 isA Role",
    "User1 hasRoleInGroup U1G2R1", "U1G2R1 hasRole Role1",
    "U1G2R1 hasGroup Group2", "User1 hasRoleInGroup U1G1R2",
    "U1G1R2 hasRole Role2", "U1G1R2 hasGroup Group1")

  def section = "cookbook"

  override protected def getGraphvizStyle: GraphStyle = {
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  }

  @Test def findGroups() {
    testQuery(
      title = "Find Groups",
      text = """To find out in what roles a user is for a particular groups (here
        'Group2'), the following Cypher Query can traverse this HyperEdge node and
        provide answers.""",
      queryText = "start n=node:node_auto_index(name = \"User1\") " +
        "match n-[:hasRoleInGroup]->hyperEdge-[:hasGroup]->group, " +
        "hyperEdge-[:hasRole]->role " +
        "where group.name = \"Group2\" " +
        "return role.name",
      returns = "The role of +User1+:",
      (p) => assertEquals(Map("role.name" -> "Role1"), p.toList.head))
  }

  @Test def findAllGroupsForAUser() {
    testQuery(
      title = "Find all groups and roles for a user",
      text = """Here, find all groups and the roles a user has, sorted by the roles names.""",
      queryText = "start n=node:node_auto_index(name = \"User1\") " +
        "match n-[:hasRoleInGroup]->hyperEdge-[:hasGroup]->group, " +
        "hyperEdge-[:hasRole]->role " +
        "return role.name, group.name " +
        "order by role.name asc",
      returns = "The groups and roles of +User1+",
      (p) => {
        val result = p.toList
        assertEquals(Map("role.name" -> "Role1", "group.name" -> "Group2"), result.head)
        assertEquals(Map("role.name" -> "Role2", "group.name" -> "Group1"), result.tail.head)
      })
  }
}
