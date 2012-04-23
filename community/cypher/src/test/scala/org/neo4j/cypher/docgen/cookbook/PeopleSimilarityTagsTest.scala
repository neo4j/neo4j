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


class PeopleSimilarityTagsTest extends DocumentingTestBase {
  def graphDescription = List("Joe favorite Cats", "Joe favorite Horses", "Joe favorite Bikes","Joe favorite Surfing", 
      "Cats tagged Animals", 
      "Horses tagged Animals", 
      "Surfing tagged Hobby", 
      "Bikes tagged Hobby", 
      "Derrick favorite Bikes", 
      "Sara favorite Bikes", 
      "Sara favorite Horses")

  def section = "cookbook"

  @Test def peopleSimilarityTags() {
    testQuery(
      title = "Find people based on similar tagged favorties",
      text = """To find out people similar to me based on taggings of their favorited items, an approach could be:
* Determine the tags associated with what I favorite.
* What else is tagged with those tags?
* Who favorites items tagged with the same tags.
* Sort the result by how many of the same things these people like.""",
      queryText = "START me=node(%Joe%) " +
      		"MATCH me-[:favorite]->myFavorites-[:tagged]->tag<-[:tagged]-theirFavorites<-[:favorite]-people " +
      		"WHERE NOT(me=people) " +
      		"RETURN people.name as name, count(*) as similar_favs " +
      		"ORDER BY similar_favs DESC",
      returns = "The list of possible friends ranked by them liking similar stuff that are not yet friends.",
      (p) => assertEquals(List(Map("name" -> "Sara", "similar_favs" -> 2),
          Map("name" -> "Derrick", "similar_favs" -> 1)), p.toList))
  } 
}
