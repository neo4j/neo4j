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


class PathTreeTest extends DocumentingTestBase {
  override def graphDescription = List(
            "Root 2010 Y10", 
            "Root 2011 Y11", 
            "Y10 12 Y10M12", 
            "Y11 01 Y11M01", 
            "Y10M12 31 Y10M12D31", 
            "Y11M01 01 Y11M01D01", 
            "Y11M01 02 Y11M11D02", 
            "Y11M01 03 Y11M12D03", 
            "Y10M12D31 NEXT Y11M01D01",
            "Y11M01D01 NEXT Y11M11D02",
            "Y11M11D02 NEXT Y11M12D03",
            "Y10M12D31 VALUE Event1",
            "Y10M12D31 VALUE Event2", 
            "Y11M01D01 VALUE Event2", 
            "Y11M12D03 VALUE Event3")

  def section = "cookbook"

  @Test def allEvents() {
    testQuery(
      title = "Return the full range",
      text = """In this case, the range goes from the first to the last leaf of the index tree. Here, 
+startPath+ (color +Greenyellow+) and +endPath+  (color +Green+) span up the range, +valuePath+  (color +Blue+) is then connecting the leafs, and the values can
be read from the +middle+ node, hanging off the +values+ (color +Red+) path.

.Graph
include::includes/path-tree-layout-full-range.asciidoc[]

""",
      queryText = "MATCH " +
                "startPath=(root)-[:`2010`]->()-[:`12`]->()-[:`31`]->(startLeaf), " +
                "endPath=(root)-[:`2011`]->()-[:`01`]->()-[:`03`]->(endLeaf), " +
                "valuePath=(startLeaf)-[:NEXT*0..]->(middle)-[:NEXT*0..]->(endLeaf), " +
                "vals=(middle)-[:VALUE]->(event) " +
                "WHERE root.name = 'Root'" +
                "RETURN event.name " +
                "ORDER BY event.name ASC",
      optionalResultExplanation = "Returning all events between 2010-12-31 and 2011-01-03, in this case all events.",
      assertions = (p) => assertEquals(List(Map("event.name" -> "Event1"),
          Map("event.name" -> "Event2"),
          Map("event.name" -> "Event2"),
          Map("event.name" -> "Event3")
          ),p.toList))
  } 
  @Test def singleDate() {
    testQuery(
      title = "Return zero range",
      text = """Here, only the events indexed under one leaf (2010-12-31) are returned.
The query only needs one path segment +rootPath+  (color +Green+) through the index.

.Graph
include::includes/path-tree-layout-zero-range.asciidoc[]

""",
      queryText = "MATCH " +
                "rootPath=(root)-[:`2010`]->()-[:`12`]->()-[:`31`]->(leaf), " +
                "(leaf)-[:VALUE]->(event) " +
                "WHERE root.name = 'Root'" +
                "RETURN event.name " +
                "ORDER BY event.name ASC",
      optionalResultExplanation = "Returning all events on the date 2010-12-31, in this case +Event1+ and +Event2+",
      assertions = (p) => assertEquals(List(Map("event.name" -> "Event1"),
          Map("event.name" -> "Event2")
          ),p.toList))
  } 
  
  @Test def sharedRoot() {
    testQuery(
      title = "Return partly shared path ranges",
      text = """Here, the query range results in partly shared paths when querying the index,
making the introduction of and common path segment +commonPath+ (color +Black+) necessary, before spanning up +startPath+ (color +Greenyellow+) and 
+endPath+ (color +Darkgreen+) . After that, +valuePath+ (color +Blue+) connects the leafs and the indexed values are returned off +values+ (color +Red+)  path.

.Graph
include::includes/path-tree-layout-shared-root-path.asciidoc[]

""",
      queryText = "MATCH " +
                "commonPath=(root)-[:`2011`]->()-[:`01`]->(commonRootEnd), " +
                "startPath=(commonRootEnd)-[:`01`]->(startLeaf), " +
                "endPath=(commonRootEnd)-[:`03`]->(endLeaf), " +
                "valuePath=(startLeaf)-[:NEXT*0..]->(middle)-[:NEXT*0..]->(endLeaf), " +
                "vals=(middle)-[:VALUE]->(event) " +
                "WHERE root.name = 'Root'" +
                "RETURN event.name " +
                "ORDER BY event.name ASC",
      optionalResultExplanation = "Returning all events between 2011-01-01 and 2011-01-03, in this case +Event2+ and +Event3+.",
      assertions = (p) => assertEquals(List(
          Map("event.name" -> "Event2"),
          Map("event.name" -> "Event3")
          ),p.toList))
  } 
}
