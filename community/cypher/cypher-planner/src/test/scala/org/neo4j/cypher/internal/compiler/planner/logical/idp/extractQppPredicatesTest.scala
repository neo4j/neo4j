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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQPPPredicates.ExtractedPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQPPPredicates.ExtractedPredicates
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class extractQppPredicatesTest extends CypherFunSuite with AstConstructionTestSupport {

  val `(a) ((n)-[r]->(m))+ (b)` : QuantifiedPathPattern = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"n_inner", v"a"),
    rightBinding = NodeBinding(v"m_inner", v"b"),
    patternRelationships =
      NonEmptyList(PatternRelationship(v"r_inner", (v"n_inner", v"m_inner"), OUTGOING, Seq.empty, SimplePatternLength)),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.unlimited),
    nodeVariableGroupings = Set(variableGrouping(v"n_inner", v"n"), variableGrouping(v"m_inner", v"m")),
    relationshipVariableGroupings = Set(variableGrouping(v"r_inner", v"r"))
  )

  val `(a) ((o)-[s]->(p))+ (b)` : QuantifiedPathPattern = QuantifiedPathPattern(
    leftBinding = NodeBinding(v"o_inner", v"a"),
    rightBinding = NodeBinding(v"p_inner", v"b"),
    patternRelationships =
      NonEmptyList(PatternRelationship(v"s_inner", (v"o_inner", v"p_inner"), OUTGOING, Seq.empty, SimplePatternLength)),
    argumentIds = Set.empty,
    selections = Selections.empty,
    repetition = Repetition(1, UpperBound.unlimited),
    nodeVariableGroupings = Set(variableGrouping(v"o_inner", v"o"), variableGrouping(v"p_inner", v"p")),
    relationshipVariableGroupings = Set(variableGrouping(v"s_inner", v"s"))
  )

  test("should extract ForAllRepetitions predicates into QPP") {
    val innerPredicate = in(varFor("r_inner"), varFor("s"))
    // Scenario: We have solve the QPP containing o-s-p first. Therefore, we could inline this predicate.
    val predicate_r_in_s = ForAllRepetitions(
      `(a) ((n)-[r]->(m))+ (b)`,
      innerPredicate
    )
    extractQPPPredicates(
      Seq(predicate_r_in_s),
      `(a) ((n)-[r]->(m))+ (b)`.variableGroupings,
      Set(varFor("o"), varFor("s"), varFor("p"))
    ) should equal(
      ExtractedPredicates(
        Set(varFor("s")),
        Seq(ExtractedPredicate(predicate_r_in_s, innerPredicate))
      )
    )
  }

  test("should not extract ForAllRepetitions predicates from other QPP") {
    // Scenario: We have solved the QPP containing n-r-m first and could therefore not inline the predicate as we do not have "s" yet.
    // However, we cannot inline it into the second QPP either because it iterates over r instead of s.
    val predicate_r_in_s = ForAllRepetitions(
      `(a) ((n)-[r]->(m))+ (b)`,
      in(varFor("r_inner"), varFor("s"))
    )
    extractQPPPredicates(
      Seq(predicate_r_in_s),
      `(a) ((o)-[s]->(p))+ (b)`.variableGroupings,
      Set(varFor("n"), varFor("r"), varFor("m"))
    ) should equal(
      ExtractedPredicates(Set.empty, Seq.empty)
    )
  }
}
