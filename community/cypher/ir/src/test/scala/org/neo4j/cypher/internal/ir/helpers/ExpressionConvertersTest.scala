/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.should.Matchers.equal

class ExpressionConvertersTest extends AnyFunSuite with AstConstructionTestSupport {

  test("should convert predicates in the right order") {
    val expression = Ands(Seq(
      hasLabels(varFor("n"), "L", "N"),
      hasTypes("m", "P"),
      propGreaterThan("n", "prop", 42),
      hasLabels(varFor("o"), "L", "N"),
      hasTypes("p", "P"),
      propGreaterThan("o", "prop", 42)
    ))(pos)

    expression.asPredicates should equal(
      Set(
        Predicate(Set(v"n"), hasLabels("n", "L")),
        Predicate(Set(v"n"), hasLabels("n", "N")),
        Predicate(Set(v"m"), hasTypes("m", "P")),
        Predicate(Set(v"n"), propGreaterThan("n", "prop", 42)),
        Predicate(Set(v"o"), hasLabels("o", "L")),
        Predicate(Set(v"o"), hasLabels("o", "N")),
        Predicate(Set(v"p"), hasTypes("p", "P")),
        Predicate(Set(v"o"), propGreaterThan("o", "prop", 42))
      )
    )
  }
}
