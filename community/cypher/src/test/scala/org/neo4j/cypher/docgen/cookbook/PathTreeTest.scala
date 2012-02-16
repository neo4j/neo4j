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


class PathTreeTest extends DocumentingTestBase {
  def graphDescription = List(
            "Root 2010 Y10", 
            "Root 2011 Y11", 
            "Y10 NEXT Y11",
            "Y10 12 Y10M12", 
            "Y11 12 Y11M12", 
            "Y10M12 NEXT Y11M12",
            "Y10M12 25 Y10M12D25", 
            "Y11M12 25 Y11M12D25", 
            "Y11M12 30 Y11M12D30", 
            "Y10M12D25 NEXT Y11M12D25",
            "Y11M12D25 NEXT Y11M12D30",
            "Y10M12D25 VALUE Event1",
            "Y10M12D25 VALUE Event2", 
            "Y11M12D25 VALUE Event2", 
            "Y11M12D30 VALUE Event3")

  def section = "cookbook"

  @Test def allEvents() {
    testQuery(
      title = "Return the full range",
      text = """In this case, the range goes from the first to the last leaf of the index tree. Here, 
        +startPath+ and +endPath+ span up the range, +valuePath+ is then connecting the leafs, and the values can
         be read from teh +middle+ node.""",
      queryText = "START root=node:node_auto_index(name = 'Root') " +
                "MATCH " +
                "startPath=root-[:`2010`]->()-[:`12`]->()-[:`25`]->startLeaf, " +
                "endPath=root-[`:2011`]->()-[:`12`]->()-[:`30`]->endLeaf, " +
                "valuePath=startLeaf-[:NEXT*0..]->middle-[:NEXT*0..]->endLeaf, " +
                "middle-[:VALUE]->event " +
                "RETURN event.name " +
                "ORDER BY event.name ASC",
      returns = "Returning all events between 2010-12-25 and 2011-12-30, in this case all events.",
      (p) => assertEquals(List(Map("event.name" -> "Event1"),
          Map("event.name" -> "Event2"),
          Map("event.name" -> "Event2"),
          Map("event.name" -> "Event3")
          ),p.toList))
  } 
  @Test def singleDate() {
    testQuery(
      title = "Return zero range",
      text = """Here, only the events indexed with on the same leaf (2010-12-25) are returned.
        The query only needs one path segment (+rootPath+) through the index.""",
      queryText = "START root=node:node_auto_index(name = 'Root') " +
                "MATCH " +
                "rootPath=root-[:`2010`]->()-[:`12`]->()-[:`25`]->leaf, " +
                "leaf-[:VALUE]->event " +
                "RETURN event.name " +
                "ORDER BY event.name ASC",
      returns = "Returning all events on the date 2010-12-25, in this case +Event1+ and +Event2+",
      (p) => assertEquals(List(Map("event.name" -> "Event1"),
          Map("event.name" -> "Event2")
          ),p.toList))
  } 
  
  @Test def sharedRoot() {
    testQuery(
      title = "Return partly shared path ranges",
      text = """Here, the query range results in partly shared paths when querying the index,
        making the introduction of and common path segment +commonPath+ necessary, before spanning up +startPath+ and 
        +endPath+. After that, +valuePath+ connects the leafs and the indexed values are returned off +middle+.""",
      queryText = "START root=node:node_auto_index(name = 'Root') " +
                "MATCH " +
                "commonPath=root-[:`2011`]->()-[:`12`]->commonRootEnd, " +
                "startPath=commonRootEnd-[:`25`]->startLeaf, " +
                "endPath=commonRootEnd-[:`30`]->endLeaf, " +
                "valuePath=startLeaf-[:NEXT*0..]->middle-[:NEXT*0..]->endLeaf, " +
                "middle-[:VALUE]->event " +
                "RETURN event.name " +
                "ORDER BY event.name ASC",
      returns = "Returning all events between 2011-12-25 and 2011-12-30, in this case +Event2+ and +Event3+.",
      (p) => assertEquals(List(
          Map("event.name" -> "Event2"),
          Map("event.name" -> "Event3")
          ),p.toList))
  } 
}
