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
import org.junit.Ignore
import org.junit.Before


class MutualFriendsAndGroupsTest extends DocumentingTestBase {
  def graphDescription = List(
      "Joe member_of_group Group1", 
      "Bob member_of_group Group1", 
      "Bill member_of_group Group1", 
      "Jill member_of_group Group1", 
      "Joe knows Bill", 
      "Jill knows Bill")

  def section = "cookbook"
    
  @Test def peopleSimilarityTags() {
    testQuery(
      title = "Find mutual friends and groups",
      text = """In this scenario, the problem is to determine mutual friends and groups, if any,
between persons. If no mutual groups or friends are found, there should be a 0 returned.""",
      queryText = "START me=node(%Joe%), other=node(%Jill%, %Bob%) " +
      		"MATCH " +
      		"pGroups=me-[?:member_of_group]->mg<-[?:member_of_group]-other, " +
      		"pMutualFriends=me-[?:knows]->mf<-[?:knows]-other " +
            "RETURN other.name as name, count(distinct pGroups) AS mutualGroups, count(distinct pMutualFriends) AS mutualFriends " +
            "ORDER By mutualFriends DESC",
      returns = "The list of mutual groups and friends for the given persons.",
      (p) => assertEquals(List(Map("name" -> "Jill", "mutualGroups" -> 1, "mutualFriends" -> 1),
          Map("name" -> "Bob", "mutualGroups" -> 1, "mutualFriends" -> 0)), p.toList))
  } 
}
