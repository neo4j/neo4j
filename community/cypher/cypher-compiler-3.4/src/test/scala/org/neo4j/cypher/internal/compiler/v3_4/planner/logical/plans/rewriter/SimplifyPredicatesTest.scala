/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.NonEmptyList
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.v3_3.logical.plans.{Argument, LogicalPlan, Selection}

class SimplifyPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("should rewrite WHERE x.prop in [1] to WHERE x.prop = 1") {
    val singleRow: LogicalPlan = Argument(Set(IdName("a")))(solved)(Map.empty)
    val predicate: Expression = In(Property(varFor("x"), PropertyKeyName("prop")(pos))(pos), ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos)))(pos))(pos)
    val cleanPredicate: Expression = Equals(Property(varFor("x"), PropertyKeyName("prop")(pos))(pos), SignedDecimalIntegerLiteral("1")(pos))(pos)
    val selection = Selection(Seq(predicate), singleRow)(solved)

    selection.endoRewrite(simplifyPredicates) should equal(
      Selection(Seq(cleanPredicate), singleRow)(solved))
  }

  test("should not rewrite WHERE x.prop in [1, 2]") {
    val singleRow: LogicalPlan = Argument(Set(IdName("a")))(solved)(Map.empty)
    val collection = ListLiteral(Seq(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("2")(pos)))(pos)
    val orgPredicate: Expression = In(Property(varFor("x"), PropertyKeyName("prop")(pos))(pos), collection)(pos)
    val selection = Selection(Seq(orgPredicate), singleRow)(solved)

    selection.endoRewrite(simplifyPredicates) should equal(selection)
  }

  test("should rewrite WHERE AndedPropertyInequality(x.prop, 1) to WHERE x.prop > 42") {
    val singleRow: LogicalPlan = Argument(Set(IdName("x")))(solved)(Map.empty)
    val variable = Variable("x")(pos)
    val property = Property(variable, PropertyKeyName("prop")(pos))(pos)
    val greaterThan = GreaterThan(property, SignedDecimalIntegerLiteral("42")(pos))(pos)
    val complexForm = AndedPropertyInequalities(variable, property, NonEmptyList(greaterThan))
    val selection = Selection(Seq(complexForm), singleRow)(solved)
    val expectedSelection = Selection(Seq(greaterThan), singleRow)(solved)

    selection.endoRewrite(simplifyPredicates) should equal(expectedSelection)
  }
}
