/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.simplifyPredicates
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class SimplifyPredicatesTest extends CypherFunSuite with PredicateTestSupport {

  val rewriter = simplifyPredicates

  test("double negation is removed") {
    not(not(P)) <=> P
  }

  test("double negation is removed by keeping an extra not") {
    not(not(not(P))) <=> not(P)
  }

  test("repeated double negation is removed") {
    not(not(not(not(P)))) <=> P
  }
}
