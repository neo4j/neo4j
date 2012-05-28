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

class LinkedListTest extends DocumentingTestBase {
  def graphDescription = List()
  def section = "cookbook"

  @Test def completeGraph() {
    testQuery(
      title = "Insert a new value into a linked list",
      text =
"""In this example, a new value is inserted at the right position into an existing Linked List.
        
Firstly, a new linked list, containing the nodes with values `0` and `2` is created in the first
subquery.

In the second part, we are looking for the node in the position before our new node will have, and the node that will come after.
They will already be connected with each other with the relationship identifier `old`.
The 0 in `zero-[:LINK*0..]->before` means that there are zero or more relationships between
the root node and the before node. Having zero relationships between two node in a path means that they are the same
node. When the linked list is empty, the `root`, `before` and `after` identifier will all point to the same node -
 the newly created root node. The self relationship will be in the `old` identifier.

In the last query part, the `old` relationship is deleted.""",
      queryText = """CREATE zero={name:0,value:0}, two={value:2,name:2}, zero-[:LINK]->two-[:LINK]->zero
==== zero ====
MATCH zero-[:LINK*0..]->before,
      after-[:LINK*0..]->zero,
      before-[old:LINK]->after
WHERE before.value? <= 1 AND
      1 <= after.value?
CREATE newValue={name:1,value : 1},
       before-[:LINK]->newValue,
       newValue-[:LINK]->after
DELETE old 
==== zero ====
MATCH p = zero-[:LINK*1..]->zero 
RETURN length(p) as list_length""",
      returns =
"""The length of the full list.""",
      assertions = (p) => assertEquals(List(Map("list_length" -> 3)),p.toList))
  } 
}
