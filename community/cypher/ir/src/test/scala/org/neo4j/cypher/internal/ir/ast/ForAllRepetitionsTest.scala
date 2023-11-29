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
package org.neo4j.cypher.internal.ir.ast

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ForAllRepetitionsTest extends CypherFunSuite with AstConstructionTestSupport {

  // (start) ((x)-[r]->(y)-[q]->(z))* (end)
  private val qpp = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"x", v"start"),
    rightBinding = NodeBinding(v"z", v"end"),
    patternRelationships = NonEmptyList(
      PatternRelationship(
        v"r",
        (v"x", v"y"),
        SemanticDirection.OUTGOING,
        Seq.empty,
        SimplePatternLength
      ),
      PatternRelationship(
        v"q",
        (v"y", v"z"),
        SemanticDirection.OUTGOING,
        Seq.empty,
        SimplePatternLength
      )
    ),
    argumentIds = Set(v"argument"),
    selections = Selections.empty,
    repetition = Repetition(0, UpperBound.Unlimited),
    nodeVariableGroupings = Set(
      variableGrouping(v"x", v"xGroup"),
      variableGrouping(v"y", v"yGroup"),
      variableGrouping(v"z", v"zGroup")
    ),
    relationshipVariableGroupings = Set(
      variableGrouping(v"r", v"rGroup"),
      variableGrouping(v"q", v"qGroup")
    )
  )

  private def rangeForGroupVariable(groupVar: String): FunctionInvocation = {
    function(
      "range",
      literalUnsignedInt(0),
      subtract(
        size(varFor(groupVar)),
        literalUnsignedInt(1)
      )
    )
  }

  test("predicate with singleton variable") {
    val predicate = greaterThan(prop("z", "prop"), prop(varFor("argument"), "prop"))
    val far = ForAllRepetitions(qpp, predicate)

    far.dependencies shouldBe Set(varFor("zGroup"), varFor("argument"))

    val iterVar = varFor("  UNNAMED0")
    far.asAllIterablePredicate(new AnonymousVariableNameGenerator()) shouldBe {
      allInList(
        iterVar,
        rangeForGroupVariable("zGroup"),
        greaterThan(
          prop(
            containerIndex(varFor("zGroup"), iterVar),
            "prop"
          ),
          prop(varFor("argument"), "prop")
        )
      )
    }
  }

  test("predicate with multiple singleton variables") {
    val predicate = greaterThan(prop("z", "prop"), prop("y", "prop"))
    val far = ForAllRepetitions(qpp, predicate)

    far.dependencies shouldBe Set(varFor("zGroup"), varFor("yGroup"))

    val iterVar = varFor("  UNNAMED0")
    far.asAllIterablePredicate(new AnonymousVariableNameGenerator()) shouldBe {
      allInList(
        iterVar,
        rangeForGroupVariable("yGroup"),
        greaterThan(
          prop(
            containerIndex(varFor("zGroup"), iterVar),
            "prop"
          ),
          prop(
            containerIndex(varFor("yGroup"), iterVar),
            "prop"
          )
        )
      )
    }
  }

  test("predicate without dependencies") {
    val predicate = greaterThan(literal(123), parameter("param", CTAny))
    val far = ForAllRepetitions(qpp, predicate)

    far.dependencies shouldBe Set(varFor("qGroup"))

    val iterVar = varFor("  UNNAMED0")
    far.asAllIterablePredicate(new AnonymousVariableNameGenerator()) shouldBe {
      allInList(iterVar, rangeForGroupVariable("qGroup"), predicate)
    }
  }
}
