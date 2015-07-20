/*
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.api.PropertyValueComparison

class NullOrderingTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v2_3.MinMaxOrdering._

  val ordering = Ordering.comparatorToOrdering(PropertyValueComparison.COMPARE_NUMBERS)

  test("Should be able to put nulls first") {
    Seq[Number](null, 4.0d, -3, null, 12).sorted(ordering.withNullsFirst) should equal(Seq[Number](null, null, -3, 4.0d, 12))
  }

  test("Should be able to put nulls last") {
    Seq[Number](null, 4.0d, -3, null, 12).sorted(ordering.withNullsLast) should equal(Seq[Number](-3, 4.0d, 12, null, null))
  }
}
