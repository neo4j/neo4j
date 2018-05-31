/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.opencypher.v9_0.expressions.SemanticDirection
import org.neo4j.values.storable.Values.NO_VALUE

class PatternComprehensionTest extends CypherFunSuite {

  val getA = Variable("a")
  val getB = Variable("b")

  test("null in null out - start node is null") {
    val aTob: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, SemanticDirection.OUTGOING)
    val patternComprehension = PathExpression(Seq(aTob), True(), getB, allowIntroducingNewIdentifiers = true)
    val state = QueryStateHelper.empty

    val ctx = ExecutionContext.empty.set("a", NO_VALUE)

    val a = patternComprehension(ctx, state)

    a should equal(NO_VALUE)
  }

  test("null in null out - end node is null") {
    val aTob: RelatedTo = RelatedTo("a", "b", "r", Seq.empty, SemanticDirection.OUTGOING)
    val patternComprehension = PathExpression(Seq(aTob), True(), getB, allowIntroducingNewIdentifiers = true)
    val state = QueryStateHelper.empty
    val ctx = ExecutionContext.empty.set("b", NO_VALUE)

    val a = patternComprehension(ctx, state)

    a should equal(NO_VALUE)
  }
}
