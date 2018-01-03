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

class NewsFeedTest extends DocumentingTestBase {

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section = "cookbook"
  override val noTitle = true;

  override val setupQueries = List("""
create
(bob{name:'Bob'})-[:STATUS]->(bob_s1{name:'bob_s1', text:'bobs status1',date:1})-[:NEXT]->(bob_s2{name:'bob_s2', text:'bobs status2',date:4}),
(alice{name:'Alice'})-[:STATUS]->(alice_s1{name:'alice_s1', text:'Alices status1',date:2})-[:NEXT]->(alice_s2{name:'alice_s2', text:'Alices status2',date:5}),
(joe{name:'Joe'})-[:STATUS]->(joe_s1{name:'joe_s1', text:'Joe status1',date:3})-[:NEXT]->(joe_s2{name:'joe_s2', text:'Joe status2',date:6}),
(joe)-[:FRIEND{status:'CONFIRMED'}]->(bob),
(alice)-[:FRIEND{status:'PENDING'}]->(joe),
(bob)-[:FRIEND{status:'CONFIRMED'}]->(alice)
""")

  @Test def timelineSearch() {
    testQuery(
      title = "Retrieve the ordered timeline of status updates of all my friends",
      text =
"""
Implementation of newsfeed or timeline feature is a frequent requirement for social applications.
The following exmaples are inspired by https://web.archive.org/web/20121102191919/http://techfin.in/2012/10/newsfeed-feature-powered-by-neo4j-graph-database/[Newsfeed feature powered by Neo4j Graph Database].
The query asked here is:

Starting at `me`, retrieve the time-ordered status feed of the status updates of me and and all friends that are connected via a `CONFIRMED FRIEND` relationship to me.""",
      queryText = """MATCH (me {name: 'Joe'})-[rels:FRIEND*0..1]-(myfriend)
WHERE ALL(r in rels WHERE r.status = 'CONFIRMED')
WITH myfriend
MATCH (myfriend)-[:STATUS]-(latestupdate)-[:NEXT*0..1]-(statusupdates)
RETURN myfriend.name as name, statusupdates.date as date, statusupdates.text as text
ORDER BY statusupdates.date DESC LIMIT 3""",
      optionalResultExplanation =
"""
To understand the strategy, let's divide the query into five steps:

. First Get the list of all my friends (along with me) through `FRIEND` relationship (`MATCH (me {name: 'Joe'})-[rels:FRIEND*0..1]-(myfriend)`). Also,  the `WHERE` predicate can be added to check whether the friend request is pending or confirmed.
. Get the latest status update of my friends through Status relationship (`MATCH (myfriend)-[:STATUS]-(latestupdate)`).
. Get subsequent status updates (along with the latest one) of my friends through `NEXT` relationships (`MATCH (myfriend)-[:STATUS]-(latestupdate)-[:NEXT*0..1]-(statusupdates)`) which will give you the latest and one additional statusupdate; adjust `0..1` to whatever suits your case.
. Sort the status updates by posted date (`ORDER BY statusupdates.date DESC`).
. `LIMIT` the number of updates you need in every query (`LIMIT 3`).""",
      assertions = (p) => assertEquals(List(Map("name" -> "Joe", "date" -> 6, "text" -> "Joe status2"),
          Map("name" -> "Bob", "date" -> 4, "text" -> "bobs status2"), Map("name" -> "Joe", "date" -> 3, "text" -> "Joe status1")), p.toList))
  }
}
