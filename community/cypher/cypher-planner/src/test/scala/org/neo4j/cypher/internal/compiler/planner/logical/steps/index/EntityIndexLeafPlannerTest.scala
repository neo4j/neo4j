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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsBoundingBoxSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsDistanceSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexRequirement
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NonSeekablePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.NotExactPredicate
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialDistanceSeekWrapper
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.ExistenceQueryExpression
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTPointNotNull
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTStringNotNull
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.IndexQuery.IndexQueryType

class EntityIndexLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2
    with AstConstructionTestSupport {

  private def queryGraph(predicateExpressions: Set[Expression]) = QueryGraph(
    selections = Selections(predicateExpressions.map(Predicate(Set(), _)))
  )

  private val leafPlanner = new IndexCompatiblePredicatesProvider {

    override protected def implicitIndexCompatiblePredicates(
      planContext: PlanContext,
      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
      predicates: Set[Expression],
      explicitCompatiblePredicates: Set[IndexCompatiblePredicate],
      valid: (LogicalVariable, Set[LogicalVariable]) => Boolean
    ): Set[IndexCompatiblePredicate] = Set.empty
  }

  private val property: Property = prop("n", "prop")
  private val property2: Property = prop("n", "prop2")
  private val integerLiteral: SignedDecimalIntegerLiteral = literalInt(1)
  private val stringLiteral: StringLiteral = literalString("hello")
  private val pointLiteral: Expression = point(1, 2)
  private val integerListLiteral: ListLiteral = listOfInt(1, 2)

  testFindIndexCompatiblePredicate(
    "equals",
    equals(property, integerLiteral),
    propertyTypes = Map(integerLiteral -> CTInteger.invariant),
    expectedPredicatesOrArgs = Args(
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)),
      cypherType = CTInteger
    )
  )

  testFindIndexCompatiblePredicate(
    "equals with unknown variable",
    equals(property, varFor("m")),
    expectedPredicatesOrArgs = Args(
      solvedPredicate = Some(PartialPredicateWrapper(isNotNull(property), _)),
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
      cypherType = CTAny
    )
  )

  testFindIndexCompatiblePredicate(
    "equals with known variable",
    equals(property, varFor("m")),
    argumentIds = Set("m"),
    expectedPredicatesOrArgs = Args(
      dependencies = Set("m"),
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)),
      cypherType = CTAny
    )
  )

  testFindIndexCompatiblePredicate(
    "equals reverse",
    equals(integerLiteral, property),
    propertyTypes = Map(integerLiteral -> CTInteger.invariant),
    expectedPredicatesOrArgs = Args(
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)),
      cypherType = CTInteger
    )
  )

  testFindIndexCompatiblePredicate(
    "in",
    in(property, integerListLiteral),
    propertyTypes = Map(
      integerListLiteral -> CTList(CTInteger),
      integerListLiteral.expressions(0) -> CTInteger,
      integerListLiteral.expressions(1) -> CTInteger
    ),
    expectedPredicatesOrArgs = Args(
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)),
      cypherType = CTInteger
    )
  )

  testFindIndexCompatiblePredicate(
    "startsWith",
    startsWith(property, literalString("test")),
    expectedPredicatesOrArgs = Args(
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.STRING_PREFIX)),
      cypherType = CTString
    )
  )

  testFindIndexPredicateOnStringPredicate(
    "endsWith",
    IndexQueryType.STRING_SUFFIX,
    endsWith(property, literalString("test"))
  )

  testFindIndexPredicateOnStringPredicate(
    "contains",
    IndexQueryType.STRING_CONTAINS,
    contains(property, literalString("test"))
  )

  testFindIndexCompatiblePredicate(
    "lessThan with literal",
    lessThan(property, integerLiteral),
    propertyTypes = Map(integerLiteral -> CTInteger.invariant),
    expectedPredicatesOrArgs = Args(
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.RANGE)),
      cypherType = CTInteger
    )
  )

  testFindIndexCompatiblePredicate(
    "lessThan with other property",
    lessThan(property, property2),
    expectedPredicatesOrArgs = Args(
      solvedPredicate = Some(PartialPredicateWrapper(isNotNull(property), _)),
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
      cypherType = CTAny
    )
  )

  testFindIndexCompatiblePredicate(
    "lessThan with unknown variable",
    lessThan(property, varFor("m")),
    expectedPredicatesOrArgs = Args(
      solvedPredicate = Some(PartialPredicateWrapper(isNotNull(property), _)),
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
      cypherType = CTAny
    )
  )

  testFindIndexCompatiblePredicate(
    "lessThan with known variable",
    lessThan(property, varFor("m")),
    argumentIds = Set("m"),
    propertyTypes = Map(varFor("m") -> CTInteger.invariant),
    expectedPredicatesOrArgs = Args(
      dependencies = Set("m"),
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.RANGE)),
      cypherType = CTInteger
    )
  )

  testFindIndexCompatiblePredicate(
    "isNotNull",
    isNotNull(property),
    expectedPredicatesOrArgs = Args(
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.EXISTS)),
      cypherType = CTAny
    )
  )

  // this should only work for NodeIndexLeafPlanner
  testFindIndexCompatiblePredicate(
    "hasLabel in generic leaf planner",
    hasLabels("n", "ConstraintLabel"),
    expectedPredicatesOrArgs = Args(expectToExist = false)
  )

  testFindIndexPredicateOnPointPredicate(
    "point.withinBBox",
    pointWithinBBox(property, point(1.0, 2.0), point(3.0, 4.0))
  )

  testFindIndexPredicateOnPointPredicate(
    "point.distance",
    lessThan(pointDistance(property, point(1.0, 2.0)), literal(1.0)),
    solvedPredicate = Some(PartialDistanceSeekWrapper(_))
  )

  test("implicit IS NOT NULL predicates simple case") {
    new givenConfig {
      qg = queryGraph(Set.empty)
    } withLogicalPlanningContext {
      (_, context) =>
        val implicitPredicates = EntityIndexLeafPlanner.implicitIsNotNullPredicates(
          varFor("varName"),
          context.plannerState.indexCompatiblePredicatesProviderContext.aggregatingProperties,
          Set("prop1", "prop2"),
          Set.empty
        )
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
    new givenConfig {
      qg = queryGraph(Set.empty)
    } withLogicalPlanningContext {
      (_, context) =>
        val implicitPredicates = EntityIndexLeafPlanner.implicitIsNotNullPredicates(
          varFor("varName"),
          context.plannerState.indexCompatiblePredicatesProviderContext.aggregatingProperties,
          Set.empty,
          Set.empty
        )
        implicitPredicates.size should be(0)
    }
  }

  test("should find multiple index compatible predicates for less-than with string literal") {
    new givenConfig {
      qg = queryGraph(Set.empty)
      addTypeToSemanticTable(stringLiteral, CTString.invariant)
    } withLogicalPlanningContext {
      (_, context) =>
        val expr = lessThan(property, stringLiteral)
        val compatiblePredicates = leafPlanner.findIndexCompatiblePredicates(
          Set(expr),
          Set.empty,
          context.semanticTable,
          context.staticComponents.planContext,
          context.plannerState.indexCompatiblePredicatesProviderContext
        )

        val rangeSeek = (
          Some(expr),
          Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.RANGE))
        )
        val textScan = (
          Some(PartialPredicate(IsTyped(property, CTStringNotNull)(expr.position), expr)),
          Set(
            IndexRequirement.SupportsIndexQuery(IndexQueryType.ALL_ENTRIES),
            IndexRequirement.HasType(IndexType.Text)
          )
        )

        compatiblePredicates.size shouldBe 2

        compatiblePredicates.map(p => (p.solvedPredicate, p.indexRequirements)) shouldBe
          Set(rangeSeek, textScan)
    }
  }

  test("should find multiple index compatible predicates for less-than with point literal") {
    new givenConfig {
      qg = queryGraph(Set.empty)
      addTypeToSemanticTable(pointLiteral, CTPoint.invariant)
    } withLogicalPlanningContext {
      (_, context) =>
        val expr = lessThan(property, pointLiteral)
        val compatiblePredicates = leafPlanner.findIndexCompatiblePredicates(
          Set(expr),
          Set.empty,
          context.semanticTable,
          context.staticComponents.planContext,
          context.plannerState.indexCompatiblePredicatesProviderContext
        )

        val rangeSeek = (
          Some(expr),
          Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.RANGE))
        )
        val pointScan = (
          Some(PartialPredicate(IsTyped(property, CTPointNotNull)(expr.position), expr)),
          Set(
            IndexRequirement.SupportsIndexQuery(IndexQueryType.ALL_ENTRIES),
            IndexRequirement.HasType(IndexType.Point)
          )
        )

        compatiblePredicates.size shouldBe 2

        compatiblePredicates.map(p => (p.solvedPredicate, p.indexRequirements)) shouldBe
          Set(rangeSeek, pointScan)
    }
  }

  private def testFindIndexPredicateOnStringPredicate(
    predicateName: String,
    indexQueryType: IndexQueryType,
    stringPredicate: BooleanExpression
  ): Unit = {
    val explicitPredicate = IndexCompatiblePredicate(
      variable = varFor("n"),
      property = property,
      predicate = stringPredicate,
      queryExpression = ExistenceQueryExpression(),
      predicateExactness = NonSeekablePredicate,
      solvedPredicate = Some(stringPredicate),
      dependencies = Set.empty,
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(indexQueryType)),
      cypherType = CTString
    )
    testFindIndexCompatiblePredicate(
      predicateName,
      stringPredicate,
      expectedPredicatesOrArgs = Predicates(Seq(explicitPredicate, explicitPredicate.convertToRangeScannable))
    )
  }

  private def testFindIndexPredicateOnPointPredicate(
    predicateName: String,
    pointPredicate: Expression,
    solvedPredicate: Option[Expression => PartialPredicate[Expression]] = None
  ): Unit = {
    val explicitPredicate = IndexCompatiblePredicate(
      variable = varFor("n"),
      property = property,
      predicate = pointPredicate,
      queryExpression = pointPredicate match {
        case AsBoundingBoxSeekable(seekable) => seekable.asQueryExpression
        case AsDistanceSeekable(seekable)    => seekable.asQueryExpression
        case _ => throw new IllegalArgumentException(s"Unexpected predicate: $pointPredicate")
      },
      predicateExactness = NotExactPredicate,
      solvedPredicate = solvedPredicate.map(_.apply(pointPredicate)).orElse(Some(pointPredicate)),
      dependencies = Set.empty,
      indexRequirements = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.BOUNDING_BOX)),
      cypherType = CTPoint
    )
    testFindIndexCompatiblePredicate(
      predicateName,
      pointPredicate,
      expectedPredicatesOrArgs = Predicates(Seq(explicitPredicate, explicitPredicate.convertToRangeScannable))
    )
  }

  sealed private trait ExpectedTestResult

  private case class Args(
    solvedPredicate: Option[Expression => PartialPredicate[Expression]] = None,
    dependencies: Set[String] = Set.empty,
    indexRequirements: Set[IndexRequirement] = Set(IndexRequirement.SupportsIndexQuery(IndexQueryType.RANGE)),
    cypherType: CypherType = CTAny,
    expectToExist: Boolean = true
  ) extends ExpectedTestResult

  private case class Predicates(predicates: Seq[IndexCompatiblePredicate]) extends ExpectedTestResult

  private def testFindIndexCompatiblePredicate(
    name: String,
    predicate: Expression,
    argumentIds: Set[String] = Set.empty,
    propertyTypes: Map[Expression, TypeSpec] = Map.empty,
    expectedPredicatesOrArgs: ExpectedTestResult
  ): Unit = {
    val predicates = Set(predicate)
    val exactQueryTypeReq = IndexRequirement.SupportsIndexQuery(IndexQueryType.EXACT)
    test(s"findIndexCompatiblePredicates ($name)") {
      new givenConfig {
        qg = queryGraph(predicates)
        propertyTypes.foreach(key => addTypeToSemanticTable(key._1, key._2))
        nodeConstraints = Set(("ConstraintLabel", Set("prop1")))
      }.withLogicalPlanningContext { (_, context) =>
        val compatiblePredicates = leafPlanner.findIndexCompatiblePredicates(
          predicates,
          argumentIds.map(varFor),
          context.semanticTable,
          context.staticComponents.planContext,
          context.plannerState.indexCompatiblePredicatesProviderContext
        )
        expectedPredicatesOrArgs match {
          case Predicates(expectedPredicates) =>
            compatiblePredicates should contain theSameElementsAs expectedPredicates
          case Args(solvedPredicate, dependencies, indexRequirements, cypherType, expectToExist) =>
            if (expectToExist) {
              withClue(s"$name should be recognized as index compatible predicate with the right parameters") {
                compatiblePredicates.size shouldBe 1
                compatiblePredicates.foreach { compatiblePredicate =>
                  compatiblePredicate.predicate should equal(predicate)
                  withClue("including solved predicate") {
                    val expectedSolvedPredicate = solvedPredicate.map(_.apply(predicate)).getOrElse(predicate)
                    compatiblePredicate.solvedPredicate.get should equal(expectedSolvedPredicate)
                  }
                  withClue("including exactness") {
                    compatiblePredicate.predicateExactness.isExact shouldBe
                      indexRequirements.contains(exactQueryTypeReq)
                  }
                  withClue("including dependencies") {
                    compatiblePredicate.dependencies.map(_.name) should equal(dependencies)
                  }
                  withClue("including indexRequirements") {
                    compatiblePredicate.indexRequirements shouldBe indexRequirements
                  }
                  withClue("including cypherType") {
                    compatiblePredicate.cypherType shouldBe cypherType
                  }
                }
              }
            } else {
              compatiblePredicates shouldBe empty
            }
        }
      }
    }
  }
}
