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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SelectPatternPredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private val dir = SemanticDirection.OUTGOING
  private val types = Seq.empty[RelTypeName]
  private val argName = "a"
  private val nodeName = "  UNNAMED2"
  private val relName = "  UNNAMED1"
  private val patternRel = PatternRelationship("  UNNAMED1", (argName, nodeName), dir, types, SimplePatternLength)
  private val nodeName2 = "  UNNAMED4"
  private val relName2 = "  UNNAMED3"
  private val patternRel2 = PatternRelationship(relName2, (argName, nodeName2), dir, types, SimplePatternLength)

  private val subqueryExp = ExistsIRExpression(
    RegularSinglePlannerQuery(
      QueryGraph(
        argumentIds = Set(argName),
        patternNodes = Set(argName, nodeName),
        patternRelationships =
          Set(patternRel)
      )
    ),
    varFor(""),
    s"exists((a)-[`$relName`]->(`$nodeName`))"
  )(pos, Some(Set(varFor(nodeName))), Some(Set(varFor(argName))))

  private val subqueryExp2 = ExistsIRExpression(
    RegularSinglePlannerQuery(
      QueryGraph(
        argumentIds = Set(argName),
        patternNodes = Set(argName, nodeName2),
        patternRelationships =
          Set(PatternRelationship(relName2, (argName, nodeName2), dir, types, SimplePatternLength))
      )
    ),
    varFor(""),
    s"exists((a)-[`$relName2`]->(`$nodeName2`))"
  )(pos, Some(Set(varFor(relName2), varFor(nodeName2))), Some(Set(varFor(argName))))

  test("should introduce semi apply for unsolved exclusive pattern predicate") {
    // Given
    val predicate = Predicate(Set(argName), subqueryExp)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(subqueryExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SemiApply(aPlan, inner), Set(subqueryExp))))
  }

  test("should introduce anti semi apply for unsolved exclusive negated pattern predicate") {
    val notExpr = not(subqueryExp)
    // Given
    val predicate = Predicate(Set(argName), notExpr)
    val selections = Selections(Set(predicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(notExpr), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(AntiSemiApply(aPlan, inner), Set(notExpr))))
  }

  test("should introduce select or semi apply for unsolved pattern predicates in disjunction with expressions") {
    // Given
    val equalsExp = equals(prop(argName, "prop"), literalString("42"))
    val orsExp = ors(subqueryExp, equalsExp)
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val argument = Argument(Set(varFor(argName)))
    val inner = Expand(argument, varFor(argName), dir, types, varFor(nodeName), varFor(patternRel.name), ExpandAll)

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SelectOrSemiApply(aPlan, inner, equalsExp), Set(orsExp))))
  }

  test(
    "should introduce select or anti semi apply for unsolved negated pattern predicates in disjunction with an expression"
  ) {
    // Given
    val equalsExp = equals(prop(argName, "prop"), literalString("42"))
    val orsExp = ors(not(subqueryExp), equalsExp)
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(Seq(SelectionCandidate(SelectOrAntiSemiApply(aPlan, inner, equalsExp), Set(orsExp))))
  }

  test("should introduce let semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given

    val orsExp = ors(subqueryExp, subqueryExp2)
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )
    val inner2 = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName2),
      varFor(patternRel2.name),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(
        SelectOrSemiApply(LetSemiApply(aPlan, inner, varFor("  UNNAMED1")), inner2, varFor("  UNNAMED1")),
        Set(orsExp)
      ))
    )
  }

  test("should introduce let semi apply and select or anti semi apply for multiple pattern predicates in or") {
    // Given
    val orsExp = ors(subqueryExp, not(subqueryExp2))
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )
    val inner2 = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName2),
      varFor(relName2),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(
        SelectOrAntiSemiApply(LetSemiApply(aPlan, inner, varFor("  UNNAMED1")), inner2, varFor("  UNNAMED1")),
        Set(orsExp)
      ))
    )
  }

  test("should introduce let anti semi apply and select or semi apply for multiple pattern predicates in or") {
    // Given
    val orsExp = ors(not(subqueryExp), subqueryExp2)
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )
    val inner2 = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName2),
      varFor(relName2),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(
        SelectOrSemiApply(LetAntiSemiApply(aPlan, inner, varFor("  UNNAMED1")), inner2, varFor("  UNNAMED1")),
        Set(orsExp)
      ))
    )
  }

  test(
    "should introduce let select or semi apply and select or anti semi apply for multiple pattern predicates in or"
  ) {
    // Given
    val equalsExp = equals(prop(argName, "prop"), literalString("42"))
    val orsExp = ors(equalsExp, subqueryExp, not(subqueryExp2))
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )
    val inner2 = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName2),
      varFor(relName2),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(
        SelectOrAntiSemiApply(
          LetSelectOrSemiApply(aPlan, inner, varFor("  UNNAMED1"), equalsExp),
          inner2,
          varFor("  UNNAMED1")
        ),
        Set(orsExp)
      ))
    )
  }

  test(
    "should introduce let anti select or semi apply and select or semi apply for multiple pattern predicates in or"
  ) {
    // Given
    val equalsExp = equals(prop(argName, "prop"), literalString("42"))
    val orsExp = ors(equalsExp, not(subqueryExp), subqueryExp2)
    val orPredicate = Predicate(Set(argName), orsExp)
    val selections = Selections(Set(orPredicate))

    val qg = QueryGraph(
      patternNodes = Set(argName),
      selections = selections
    )

    val context = newMockedLogicalPlanningContext(planContext = newMockedPlanContext())

    val aPlan = newMockedLogicalPlan(context.staticComponents.planningAttributes, argName)
    val inner = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName),
      varFor(patternRel.name),
      ExpandAll
    )
    val inner2 = Expand(
      Argument(Set(varFor(argName))),
      varFor(argName),
      dir,
      types,
      varFor(nodeName2),
      varFor(relName2),
      ExpandAll
    )

    // When
    val result = SelectPatternPredicates(aPlan, Set(orsExp), qg, InterestingOrderConfig.empty, context).toSeq

    // Then
    result should equal(
      Seq(SelectionCandidate(
        SelectOrSemiApply(
          LetSelectOrAntiSemiApply(aPlan, inner, varFor("  UNNAMED1"), equalsExp),
          inner2,
          varFor("  UNNAMED1")
        ),
        Set(orsExp)
      ))
    )
  }
}
