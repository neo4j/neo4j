/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
  def graphDescription = List()

  def section = "cookbook"
  override val noTitle = true;

  @Test def updateStatus() {
      executeQuery("""
create 
(bob{name:'Bob'})-[:STATUS]->(bob_s1{name:'bob_s1', text:'bobs status1',date:1})-[:NEXT]->(bob_s2{name:'bob_s2', text:'bobs status2',date:4})
          """)
    testQuery(
      title = "Insert a new status update for a user",
      text =
"""
Here, the example shows how to add a new status update into the existing data for a user.""",
      queryText = """START me=node:node_auto_index(name='Bob') MATCH me-[r?:STATUS]-secondlatestupdate DELETE r 
WITH me, secondlatestupdate 
CREATE me-[:STATUS]->(latest_update{text:'Status',date:123}) 
WITH latest_update,secondlatestupdate 
CREATE latest_update-[:NEXT]-secondlatestupdate 
WHERE secondlatestupdate <> null 
RETURN latest_update.text as new_status""",
      returns =
"""
Dividing the query into steps, this query resembles adding new item in middle of a doubly linked list:

. Get the latest update (if it exists) of the user through the `STATUS` relationship (`MATCH me-[r?:STATUS]-secondlatestupdate`).
. Delete the `STATUS` relationship between `user` and `secondlatestupdate` (if it exists), as this would become the second latest update now and only the latest update would be added through a `STATUS` relationship, all earlier updates would be connected to their subsequent updates through a `NEXT` relationship. (`DELETE r`).
. Now, create the new `statusupdate` node (with text and date as properties) and connect this with the user through a `STATUS` relationship (`CREATE me-[:STATUS]->(latest_update{text:'Status',date:123})`).
. Now, create a `NEXT` relationship between the latest status update and the second latest status update (if it exists) (`CREATE latest_update-[:NEXT]-secondlatestupdate WHERE secondlatestupdate <> null`).""",
      assertions = (p) => assertEquals(List(Map("new_status" -> "Status")), p.toList))
  } 
}
