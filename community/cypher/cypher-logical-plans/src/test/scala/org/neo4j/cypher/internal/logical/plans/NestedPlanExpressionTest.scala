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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NestedPlanExpressionTest extends CypherFunSuite {

  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)
  implicit private val idGen: IdGen = SameId(Id.INVALID_ID)

  test("nested plan collect should render nicely") {
    val e = NestedPlanExpression.collect(
      Argument(),
      Variable("foo")(InputPosition.NONE, Variable.isIsolatedDefault),
      Variable("foo")(InputPosition.NONE, Variable.isIsolatedDefault)
    )(InputPosition.NONE)
    stringifier(e) should equal("foo")
  }

  test("nested plan exists should render nicely") {
    val e = NestedPlanExpression.exists(Argument(), Variable("foo")(InputPosition.NONE, Variable.isIsolatedDefault))(
      InputPosition.NONE
    )
    stringifier(e) should equal("foo")
  }
}
