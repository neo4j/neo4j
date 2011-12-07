/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.symbols.{Identifier, NodeType, SymbolTable}
import org.neo4j.cypher.commands.EntityValue
import org.neo4j.graphdb.{Relationship, Direction, Node}
import org.neo4j.cypher.{PathImpl, GraphDatabaseTestBase}


class AllLeafPathsTest extends GraphDatabaseTestBase with Assertions {
  /*
          (a)
          /\
        (b)(c)
        /\
      (d)(e)
   */

  var a: Node = null
  var b: Node = null
  var c: Node = null
  var d: Node = null
  var e: Node = null
  var rab: Relationship = null
  var rac: Relationship = null
  var rbd: Relationship = null
  var rbe: Relationship = null

  def source = {
    a = createNode()
    b = createNode()
    c = createNode()
    d = createNode()
    e = createNode()
    rab = relate(a, b)
    rac = relate(a, c)
    rbd = relate(b, d)
    rbe = relate(b, e)
    new FakePipe(Seq(Map("root" -> a)), new SymbolTable(Identifier("root", NodeType())))
  }

  @Test def shouldReturnAllLeafs() {
    val pipe = new AllLeafPathsPipe(source, EntityValue("root"), "leaf", "path", Direction.OUTGOING, None, None)
    assert(pipe.map(m => m("leaf")).toSet === Set(d, e, c))
  }

  @Test def shouldReturnAllLeafPaths() {
    val pipe = new AllLeafPathsPipe(source, EntityValue("root"), "leaf", "path", Direction.OUTGOING, None, None)
    assert(pipe.map(m => m("path")).toSet === Set(PathImpl(a, rab, b, rbd, d), PathImpl(a, rab, b, rbe, e), PathImpl(a, rac, c)))
  }
}