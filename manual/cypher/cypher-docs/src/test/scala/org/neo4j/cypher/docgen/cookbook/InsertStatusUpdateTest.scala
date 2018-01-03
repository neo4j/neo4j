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

class InsertStatusUpdateTest extends DocumentingTestBase {

  def section = "cookbook"
  override val noTitle = true;
  
  override val setupQueries = List("""
create 
(bob{name:'Bob'})-[:STATUS]->(bob_s1{name:'bob_s1', text:'bobs status1',date:1})-[:NEXT]->(bob_s2{name:'bob_s2', text:'bobs status2',date:4})
""")

  @Test def updateStatus() {
    testQuery(
      title = "Insert a new status update for a user",
      text =
        """
Here, the example shows how to add a new status update into the existing data for a user.""",
      queryText = """
MATCH (me)
WHERE me.name='Bob'
OPTIONAL MATCH (me)-[r:STATUS]-(secondlatestupdate)
DELETE r
CREATE (me)-[:STATUS]->(latest_update {text:'Status',date:123})
WITH latest_update, collect(secondlatestupdate) as seconds
FOREACH(x in seconds | CREATE (latest_update)-[:NEXT]->(x))
RETURN latest_update.text as new_status""",
      optionalResultExplanation =
        """
Dividing the query into steps, this query resembles adding new item in middle of a doubly linked list:

. Get the latest update (if it exists) of the user through the `STATUS` relationship (`OPTIONAL MATCH (me)-[r:STATUS]-(secondlatestupdate)`).
. Delete the `STATUS` relationship between `user` and `secondlatestupdate` (if it exists), as this would become the second latest update now
  and only the latest update would be added through a `STATUS` relationship;
  all earlier updates would be connected to their subsequent updates through a `NEXT` relationship. (`DELETE r`).
. Now, create the new `statusupdate` node (with text and date as properties) and connect this with the user through a `STATUS` relationship
  (`CREATE (me)-[:STATUS]->(latest_update { text:'Status',date:123 })`).
. Pipe over `statusupdate` or an empty collection to the next query part
  (`WITH latest_update, collect(secondlatestupdate) AS seconds`).
. Now, create a `NEXT` relationship between the latest status update and the second latest status update (if it exists) (`FOREACH(x in seconds | CREATE (latest_update)-[:NEXT]->(x))`).""",
      assertions = (p) => assertEquals(List(Map("new_status" -> "Status")), p.toList))
  }
}
