/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.lab.cypher

import commands._
import org.junit.Test
import org.junit.Assert._

/**
 * Created by Andres Taylor
 * Date: 5/24/11
 * Time: 15:53 
 */

class FilterTests {
  @Test def andsDoesntHaveOrs() {
    val x = And(
      StringEquals("a", "b", "c"),
      StringEquals("a", "b", "c"))
    assertFalse("Should not claim it has any ors", x.hasOrs)
  }

  @Test def nestedStuffHasOrs() {
    val x =
      And(
        StringEquals("a", "b", "c"),
        Or(
          StringEquals("a", "b", "c"),
          StringEquals("a", "b", "c")))

    assertTrue("Does have an or, but didn't say it.", x.hasOrs)
  }
}