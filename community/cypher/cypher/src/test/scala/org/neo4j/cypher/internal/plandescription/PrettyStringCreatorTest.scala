/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.plandescription.PrettyStringCreator.PrettyStringInterpolator
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PrettyStringCreatorTest extends CypherFunSuite {

  test("should interpolate just literal string") {
    pretty"Foo" should equal(PrettyStringCreator("Foo"))
  }

  test("should interpolate with one argument") {
    val p1 = PrettyStringCreator("Bar")
    pretty"Foo$p1" should equal(PrettyStringCreator("FooBar"))
    pretty"${p1}Foo" should equal(PrettyStringCreator("BarFoo"))
  }

  test("should interpolate with two argument") {
    val p1 = PrettyStringCreator("Bar")
    val p2 = PrettyStringCreator("Baz")
    pretty"${p1}Foo$p2" should equal(PrettyStringCreator("BarFooBaz"))
    pretty"Foo$p1$p2" should equal(PrettyStringCreator("FooBarBaz"))
  }
}
