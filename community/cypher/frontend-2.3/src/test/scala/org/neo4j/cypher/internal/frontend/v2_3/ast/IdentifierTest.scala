/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticState}

class IdentifierTest extends CypherFunSuite {

  test("shouldDefineIdentifierDuringSemanticCheckWhenUndefined") {
    val position = DummyPosition(0)
    val identifier = Identifier("x")(position)

    val result = identifier.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.position should equal(position)
    result.state.symbol("x").isDefined should equal(true)
    result.state.symbolTypes("x") should equal(CTAny.covariant)
  }
}
