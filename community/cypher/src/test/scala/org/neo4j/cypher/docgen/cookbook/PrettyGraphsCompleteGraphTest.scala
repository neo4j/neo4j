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

class PrettyGraphsCompleteGraphTest extends DocumentingTestBase {
  def graphDescription = List()
  def section = "cookbook"
  generateInitialGraphForConsole = false
  override val graphvizOptions = "graph [layout=circo]"


  @Test def completeGraph() {
    testQuery(
      title = "Complete graph",
      text =
"""For this graph, a root node is created, and used to hang a number 
        of nodes from. Then, two nodes are selected, hanging from the center, with the requirement that the 
        id of the first is less than the id of the next. This is to prevent double relationships and 
        self relationships. Using said match, relationships between all these nodes are created. Lastly, 
        the center node and all relationships connected to it are removed.""",
      queryText = """create center
foreach( x in range(1,6) : 
   create leaf={count : x}, center-[:X]->leaf
)
==== center ====
MATCH leaf1<--center-->leaf2
WHERE id(leaf1)<id(leaf2)
CREATE leaf1-[:X]->leaf2
==== center ====
MATCH center-[r]->()
DELETE center,r;""",
      returns =
"""Nothing is returned by this query.""",
      assertions = (p) => assertTrue(true))
  } 
}
