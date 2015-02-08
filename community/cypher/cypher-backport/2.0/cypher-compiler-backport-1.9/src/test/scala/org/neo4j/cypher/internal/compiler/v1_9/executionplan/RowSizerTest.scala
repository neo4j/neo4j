/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan

import org.junit.{Before, Test}
import org.scalatest.Assertions
import org.neo4j.cypher.internal.compiler.v1_9.parser.CypherParser

class RowSizerTest extends Assertions {
  var sizer: RowSizer = null

  @Before def init() {
    sizer = new RowSizer
  }

  @Test def shouldFindSingleRowArrayIsEnough() {
    // GIVEN
    val q = parse("start n=node(0) return n.prop")

    //WHEN
    q.visit {
      case x => x.addsToRow().foreach(key => sizer.push(key))
    }

    //THEN
    assert(sizer.maxSize === 1)
    //    assert(q.returns.returnItems.head.expressions(null).head._2 === Property(Identifier("n"), "prop"))
  }

  @Test def findRowSizeForOneMatchedRelAndNode() {
    //GIVEN
    val q = parse("start n=node(0) match n-[r]->m return n,m,r")

    //WHEN
    q.visit {
      case x => x.addsToRow().foreach(key => sizer.push(key))
    }

    //THEN
    assert(sizer.maxSize === 3)
  }

  @Test def should_find_rows_size_for_predicate() {
    //GIVEN
    val q = parse("start n=node(*) where x.bar in [1,2,3,4] return n")

    //WHEN
    q.visit {
      case x => x.addsToRow().foreach(key => sizer.push(key))
    }

    //THEN
    assert(sizer.maxSize === 2 )
  }

  val parser = CypherParser()

  def parse(s: String): PartiallySolvedQuery = PartiallySolvedQuery(parser.parse(s))
}
