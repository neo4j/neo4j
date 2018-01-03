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
import org.neo4j.visualization.graphviz.GraphStyle
import org.neo4j.visualization.graphviz.AsciiDocSimpleStyle

class GraphityTest extends DocumentingTestBase {

  override protected def getGraphvizStyle: GraphStyle = 
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()
  
  override def graphDescription = List(
    "Joe has Joe_s1",
    "Joe_s1 next Joe_s2",
    "Bill has Bill_s1",
    "Bill_s1 next Bill_s2",
    "Ted has Ted_s1",
    "Ted_s1 next Ted_s2",
    "Bob bob_knows Ted",
    "Bob has Bob_s1",
    "Ted bob_knows Bill",
    "Jane jane_knows Bill",
    "Bill jane_knows Joe",
    "Joe jane_knows Bob")

  def section = "cookbook"

  @Test def findActivityStreams() {
    testQuery(
      title = "Find Activity Streams in a network without scaling penalty",
      text = """This is an approach for scaling the retrieval of activity streams in a friend graph put forward by Rene Pickard as http://www.rene-pickhardt.de/graphity-an-efficient-graph-model-for-retrieving-the-top-k-news-feeds-for-users-in-social-networks/[Graphity].
In short, a linked list is created for every persons friends in the order that the last activities of these friends have occured.
When new activities occur for a friend, all the ordered friend lists that this friend is part of are reordered, transferring computing load to the time of new event updates instead of activity stream reads.

[TIP]
This approach of course makes excessive use of relationship types.
This needs to be taken into consideration when designing a production system with this approach.
See <<capabilities-capacity>> for the maximum number of relationship types.

To find the activity stream for a person, just follow the linked list of the friend list, and retrieve the needed amount of activities form the respective activity list of the friends.""",
      queryText =
        "MATCH p=(me {name: 'Jane'})-[:jane_knows*]->(friend), " +
        "(friend)-[:has]->(status) " +
        "RETURN me.name, friend.name, status.name, length(p) " +
        "ORDER BY length(p)",
      optionalResultExplanation = "The returns the activity stream for Jane.",
      assertions = (p) => assertEquals(List(Map("status.name" -> "Bill_s1", "friend.name" -> "Bill", "me.name" -> "Jane", "length(p)" -> 1),
          Map("status.name" -> "Joe_s1", "friend.name" -> "Joe", "me.name" -> "Jane", "length(p)" -> 2),
          Map("status.name" -> "Bob_s1", "friend.name" -> "Bob", "me.name" -> "Jane", "length(p)" -> 3)
          ), p.toList))
  }
}
