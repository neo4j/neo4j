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

class CollabFilteringTest extends DocumentingTestBase {
  def graphDescription = List("Joe knows Bill", "Joe knows Sara", "Sara knows Bill", "Sara knows Ian", "Bill knows Derrick",
            "Bill knows Ian", "Sara knows Jill")

  def section = "cookbook"

  @Test def basicCollborativeFiltering() {
    testQuery(
      title = "Simple Friend Finder",
      text = """To find out the friends of Joes friends that are not already his friends, Cypher looks like:""",
      queryText = "start joe=node:node_auto_index(name = \"Joe\") " +
      		"match joe-[:knows]->friend-[:knows]->friend_of_friend, " +
      		"joe-[r?:knows]->friend_of_friend " +
      		"where r IS NULL " +
      		"return friend_of_friend.name, COUNT(*) " +
      		"order by COUNT(*) DESC, friend_of_friend.name",
      returns = "The list of Friends-of-friends  order by the number of connections to them, secondly by their name.",
      (p) => assertEquals(List(Map("friend_of_friend.name" -> "Ian", "count(*)" -> 2),
          Map("friend_of_friend.name" -> "Derrick", "count(*)" -> 1),
          Map("friend_of_friend.name" -> "Jill", "count(*)" -> 1)), p.toList))
  } 
}

