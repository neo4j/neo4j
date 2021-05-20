/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class EntityIndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  private def queryGraph(predicateExpressions: Set[Expression]) = QueryGraph(
    selections = Selections(predicateExpressions.map(Predicate(Set(), _)))
  )

  private val leafPlanner = new EntityIndexLeafPlanner {

    override def apply(queryGraph: QueryGraph,
                       interestingOrderConfig: InterestingOrderConfig,
                       context: LogicalPlanningContext): Seq[LogicalPlan] = ???

    override protected def implicitIndexCompatiblePredicates(context: LogicalPlanningContext,
                                                             predicates: Set[Expression],
                                                             explicitCompatiblePredicates: Set[EntityIndexLeafPlanner.IndexCompatiblePredicate],
                                                             valid: (LogicalVariable, Set[LogicalVariable]) => Boolean): Set[EntityIndexLeafPlanner.IndexCompatiblePredicate] = Set.empty
  }

  private val property: Property = prop("n", "prop")
  private val property2: Property = prop("n", "prop2")
  private val integerLiteral: SignedDecimalIntegerLiteral = literalInt(1)

  testFindIndexCompatiblePredicate("equals", equals(property, integerLiteral), isExact = true)

  testFindIndexCompatiblePredicate("equals with unknown variable", equals(property, varFor("m")), solvedPredicate = Some(isNotNull(property)))

  testFindIndexCompatiblePredicate("equals with known variable",
    equals(property, varFor("m")),
    isExact = true,
    argumentIds = Set("m"),
    dependencies = Set("m"))

  testFindIndexCompatiblePredicate("equals reverse",
    equals(integerLiteral, property),
    isExact = true,
    propertyTypes = Map(property -> CTInteger.invariant))

  testFindIndexCompatiblePredicate("in",
    in(property, listOfInt(1, 2)),
    isExact = true)

  testFindIndexCompatiblePredicate("startsWith", startsWith(property, literalString("test")))

  testFindIndexCompatiblePredicate("endsWith", endsWith(property, literalString("test")))

  testFindIndexCompatiblePredicate("contains", contains(property, literalString("test")))

  testFindIndexCompatiblePredicate("lessThan with literal",
    lessThan(property, integerLiteral),
    propertyTypes = Map(integerLiteral -> CTInteger.invariant))

  testFindIndexCompatiblePredicate("lessThan with other property",
    lessThan(property, property2),
    solvedPredicate = Some(isNotNull(property)))

  testFindIndexCompatiblePredicate("lessThan with unknown variable",
    lessThan(property, varFor("m")),
    solvedPredicate = Some(isNotNull(property)))

  testFindIndexCompatiblePredicate("lessThan with known variable",
    lessThan(property, varFor("m")),
    argumentIds = Set("m"), dependencies = Set("m"),
    propertyTypes = Map(varFor("m") -> CTInteger.invariant))

  testFindIndexCompatiblePredicate("isNotNull", isNotNull(property))

  // this should only work for NodeIndexLeafPlanner
  testFindIndexCompatiblePredicate("hasLabel in generic leaf planner", hasLabels("n", "ConstraintLabel"), expectToExist = false)

  private def testFindIndexCompatiblePredicate(name: String,
                                               predicate: Expression,
                                               solvedPredicate: Option[Expression] = None,
                                               isExact: Boolean = false,
                                               argumentIds: Set[String] = Set.empty,
                                               dependencies: Set[String] = Set.empty,
                                               propertyTypes: Map[Expression, TypeSpec] = Map.empty,
                                               expectToExist: Boolean = true): Unit = {
    val predicates = Set(predicate)
    test(s"findIndexCompatiblePredicates ($name)") {
      new given {
        qg = queryGraph(predicates)
        propertyTypes.foreach(key => addTypeToSemanticTable(key._1, key._2))
        nodeConstraints = Set(("ConstraintLabel", Set("prop1")))
      }.withLogicalPlanningContext { (_, context) =>
          val compatiblePredicates = leafPlanner.findIndexCompatiblePredicates(predicates, argumentIds, context)
          if (expectToExist) {
            withClue(s"$name should be recognized as index compatible predicate with the right parameters") {
              compatiblePredicates.size shouldBe 1
              compatiblePredicates.foreach { compatiblePredicate =>
                compatiblePredicate.predicate should equal(predicate)
                withClue("including solved predicate") {
                  val expectedSolvedPredicate = solvedPredicate.map(PartialPredicateWrapper(_, predicate)).getOrElse(predicate)
                  compatiblePredicate.solvedPredicate.get should equal(expectedSolvedPredicate)
                }
                withClue("including exactness") {
                  compatiblePredicate.predicateExactness.isExact shouldBe isExact
                }
                withClue("including dependencies") {
                  compatiblePredicate.dependencies.map(_.name) should equal(dependencies)
                }
              }
            }
          } else {
            compatiblePredicates shouldBe empty
          }
      }
    }
  }

  test("implicit IS NOT NULL predicates simple case") {
    new given {
      qg = queryGraph(Set.empty)
    } withLogicalPlanningContext {
      (_, ctx) =>
        val implicitPredicates = EntityIndexLeafPlanner.implicitIsNotNullPredicates(varFor("varName"), ctx, Set("prop1", "prop2"), Set.empty)
        implicitPredicates.size should be(2)
        implicitPredicates.foreach(predicate =>
          predicate.predicate should matchPattern {
            case IsNotNull(_) => ()
          }
        )
        implicitPredicates.map(_.propertyKeyName.name) should equal(Set("prop1", "prop2"))
    }
  }

  test("implicit IS NOT NULL predicates empty case") {
    new given {
      qg = queryGraph(Set.empty)
    } withLogicalPlanningContext {
      (_, ctx) =>
        val implicitPredicates = EntityIndexLeafPlanner.implicitIsNotNullPredicates(varFor("varName"), ctx, Set.empty, Set.empty)
        implicitPredicates.size should be(0)
    }
  }
}
