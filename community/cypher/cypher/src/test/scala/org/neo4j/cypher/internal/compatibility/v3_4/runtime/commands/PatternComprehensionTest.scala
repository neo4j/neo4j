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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.Variable
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.predicates.True
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.values.storable.Values.NO_VALUE

class PatternComprehensionTest extends CypherFunSuite {

  val getA = Variable("a")
  val getB = Variable("b")

  test("null in null out - start node is null") {
    val aTob: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, SemanticDirection.OUTGOING)
    val patternComprehension = PathExpression(Seq(aTob), True(), getB, allowIntroducingNewIdentifiers = true)
    val state = QueryStateHelper.empty

    val ctx = ExecutionContext.empty.newWith1("a", NO_VALUE)

    val a = patternComprehension(ctx, state)

    a should equal(NO_VALUE)
  }

  test("null in null out - end node is null") {
    val aTob: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, SemanticDirection.OUTGOING)
    val patternComprehension = PathExpression(Seq(aTob), True(), getB, allowIntroducingNewIdentifiers = true)
    val state = QueryStateHelper.empty
    val ctx = ExecutionContext.empty.newWith1("b", NO_VALUE)

    val a = patternComprehension(ctx, state)

    a should equal(NO_VALUE)
  }
}
