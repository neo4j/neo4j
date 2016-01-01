/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.perty

import org.neo4j.cypher.internal.commons.CypherFunSuite

class DocBuilderTest extends CypherFunSuite {
  import DocBuilder.asDocBuilder

  val dummy1 = asDocBuilder[Any](PartialFunction.empty)
  val dummy2 = asDocBuilder[Any](PartialFunction.empty)
  val dummy3 = asDocBuilder[Any](PartialFunction.empty)

  test("single orElse chain prepends") {
    dummy1 orElse SimpleDocBuilderChain[Any](dummy2) should equal(SimpleDocBuilderChain[Any](dummy1, dummy2))
  }

  test("single orElse single combines") {
    dummy1 orElse dummy2 should equal(SimpleDocBuilderChain[Any](dummy1, dummy2))
  }


  test("chain orElse single appends") {
    SimpleDocBuilderChain[Any](dummy2) orElse dummy1 should equal(SimpleDocBuilderChain[Any](dummy2, dummy1))
  }

  test("chain orElse chain appends") {
    SimpleDocBuilderChain[Any](dummy2) orElse SimpleDocBuilderChain[Any](dummy1, dummy3) should equal(SimpleDocBuilderChain[Any](dummy2, dummy1, dummy3))
  }

  test("constructs empty doc chain") {
    SimpleDocBuilderChain[Any]().nestedDocGenerator should equal(PartialFunction.empty)
  }
}
