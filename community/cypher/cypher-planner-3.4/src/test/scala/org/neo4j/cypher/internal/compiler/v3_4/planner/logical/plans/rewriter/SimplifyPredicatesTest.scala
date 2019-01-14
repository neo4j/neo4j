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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.util.v3_4.NonEmptyList
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans.{Argument, LogicalPlan, Selection}

class SimplifyPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should rewrite WHERE x.prop in [1] to WHERE x.prop = 1") {
    val argument: LogicalPlan = Argument(Set("a"))
    val predicate: Expression = In(Property(varFor("x"), PropertyKeyName("prop")(pos))(pos), ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos))(pos)
    val cleanPredicate: Expression = Equals(Property(varFor("x"), PropertyKeyName("prop")(pos))(pos), SignedDecimalIntegerLiteral("1")(pos))(pos)
    val selection = Selection(Seq(predicate), argument)

    selection.endoRewrite(simplifyPredicates) should equal(
      Selection(Seq(cleanPredicate), argument))
  }

  test("should not rewrite WHERE x.prop in [1, 2]") {
    val argument: LogicalPlan = Argument(Set("a"))
    val collection = ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("2")(pos)))(pos)
    val orgPredicate: Expression = In(Property(varFor("x"), PropertyKeyName("prop")(pos))(pos), collection)(pos)
    val selection = Selection(Seq(orgPredicate), argument)

    selection.endoRewrite(simplifyPredicates) should equal(selection)
  }

  test("should rewrite WHERE AndedPropertyInequality(x.prop, 1) to WHERE x.prop > 42") {
    val argument: LogicalPlan = Argument(Set("x"))
    val variable = Variable("x")(pos)
    val property = Property(variable, PropertyKeyName("prop")(pos))(pos)
    val greaterThan = GreaterThan(property, SignedDecimalIntegerLiteral("42")(pos))(pos)
    val complexForm = AndedPropertyInequalities(variable, property, NonEmptyList(greaterThan))
    val selection = Selection(Seq(complexForm), argument)
    val expectedSelection = Selection(Seq(greaterThan), argument)

    selection.endoRewrite(simplifyPredicates) should equal(expectedSelection)
  }
}
