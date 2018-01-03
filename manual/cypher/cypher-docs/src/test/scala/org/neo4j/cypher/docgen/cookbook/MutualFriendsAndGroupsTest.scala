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
package org.neo4j.cypher.docgen.cookbook

import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.docgen.DocumentingTestBase

class MutualFriendsAndGroupsTest extends DocumentingTestBase {
  override def graphDescription = List(
      "Joe member_of_group Group1", 
      "Bob member_of_group Group1", 
      "Bill member_of_group Group1", 
      "Jill member_of_group Group1", 
      "Joe knows Bill", 
      "Jill knows Bill")

  def section = "cookbook"
  override val noTitle = true;

  @Test def peopleSimilarityTags() {
    testQuery(
      title = "Find mutual friends and groups",
      text =
"""In this scenario, the problem is to determine mutual friends and groups, if any,
between persons. If no mutual groups or friends are found, there should be a `0` returned.""",
      queryText = "MATCH (me {name: 'Joe'}), (other) " +
          "WHERE other.name IN ['Jill', 'Bob'] " +
          "OPTIONAL MATCH pGroups=(me)-[:member_of_group]->(mg)<-[:member_of_group]-(other) \n" +
          "OPTIONAL MATCH pMutualFriends=(me)-[:knows]->(mf)<-[:knows]-(other) " +
          "RETURN other.name as name, \n count(distinct pGroups) AS mutualGroups, \n count(distinct pMutualFriends) AS mutualFriends " +
            "ORDER BY mutualFriends DESC",
      optionalResultExplanation =
"""The question we are asking is -- how many unique paths are there between me and Jill, the paths being common group memberships, and common friends.
If the paths are mandatory, no results will be returned if me and Bob lack any common friends, and we don't want that. To make a path optional,
you have to make at least one of it's relationships optional. That makes the whole path optional.""",
      assertions = (p) => assertEquals(List(Map("name" -> "Jill", "mutualGroups" -> 1, "mutualFriends" -> 1),
          Map("name" -> "Bob", "mutualGroups" -> 1, "mutualFriends" -> 0)), p.toList))
  } 
}
