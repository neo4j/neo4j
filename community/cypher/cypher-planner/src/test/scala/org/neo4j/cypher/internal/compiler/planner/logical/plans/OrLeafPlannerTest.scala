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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allRelationshipsScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.idSeekLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.RelationshipIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexSeekPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.nodeIndexStringSearchScanPlanProvider
import org.neo4j.cypher.internal.compiler.planner.logical.steps.intersectionLabelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.labelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.relationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.unionLabelScanLeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.unionRelationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.storageengine.api.AllRelationshipsScan
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.scalacheck.Shrink
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class OrLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with ScalaCheckDrivenPropertyChecks {
  import OrLeafPlannerTest._

  private lazy val expressionStringifier: ExpressionStringifier = ExpressionStringifier()

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 100)

  test("should not plan node filter disjunction on top of relationship leaf plans") {
    // Allow only NodeByLabelScan and AllRelationshipsScan leaf plans.
    val orLeafPlanner = OrLeafPlanner(Seq(
      labelScanLeafPlanner(Set.empty),
      allRelationshipsScanLeafPlanner(Set.empty)
    ))
    // Node predicate disjunction
    val predicates: Set[Expression] = Set(ors(hasLabels("n", "A"), hasLabels("n", "B")))

    new givenConfig {
      qg = QueryGraph(
        selections = Selections(predicates.flatMap(_.asPredicates)),
        patternNodes = Set(v"n", v"m"),
        patternRelationships =
          Set(PatternRelationship(v"r", (v"n", v"m"), OUTGOING, Seq(relTypeName("R")), SimplePatternLength)),
        argumentIds = Set()
      )
      // Make AllRelationshipsScan (also with Selection on top) cheap,
      // make NodeByLabelScan expensive.
      cost = {
        case (_: AllRelationshipsScan, _, _, _) => 1.0
        case (_: Selection, _, _, _)            => 1.0
        case (_: NodeByLabelScan, _, _, _)      => 1000.0
      }
    }.withLogicalPlanningContext { (cfg, ctx) =>
      // when
      val resultPlans = orLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      // then
      resultPlans shouldEqual Set(
        new LogicalPlanBuilder(wholePlan = false)
          .orderedDistinct(Seq("n"), "n AS n")
          .orderedUnion("n ASC")
          .|.nodeByLabelScan("n", "B", IndexOrderAscending)
          .nodeByLabelScan("n", "A", IndexOrderAscending)
          .build()
          /* and not
            new LogicalPlanBuilder(wholePlan = false)
            .distinct("r AS r", "n AS n", "m AS m")
            .union()
            .|.filter("n:B")
            .|.allRelationshipsScan("(n)-[r]->(m)")
            .filter("n:A")
            .allRelationshipsScan("(n)-[r]->(m)")
            .build()
           */
      )
    }
  }

  // The tests below feed non-normalized expressions to the OrLeafPlanner.
  // This is because the normalizer will skip large enough boolean expressions, so the planner should NEVER assume that the received expression is normalized.
  test(
    "should ensure that the predicates in the result match the original query even if both sides of the disjunction produce the same plan"
  ) {
    val orLeafPlanner = orleafPlannerWithSubplans
    // GIVEN  expression (a:A or a.foo IS NOT NULL) AND a:A AND NOT (NOT a.foo = 42)
    val queryGraph = QueryGraph(
      patternNodes = Set(var_a, var_b),
      patternRelationships = Set(PatternRelationship(
        var_r,
        (var_a, var_b),
        BOTH,
        Seq(RelTypeName("R")(InputPosition.NONE)),
        SimplePatternLength
      )),
      selections = Selections(Set(
        Predicate(Set(var_a), HasLabels(var_a, List(A))(InputPosition.NONE)),
        Predicate(
          Set(var_a),
          Not(Not(Equals(Property(var_a, foo)(InputPosition.NONE), fortyTwo)(InputPosition.NONE))(InputPosition.NONE))(
            InputPosition.NONE
          )
        ),
        Predicate(
          Set(var_a),
          Ors(ListSet(
            IsNotNull(Property(var_a, foo)(InputPosition.NONE))(InputPosition.NONE),
            HasLabels(var_a, List(A))(InputPosition.NONE)
          ))(InputPosition.NONE)
        )
      ))
    )

    // then the solved predicates should match the original plan
    new givenPlanWithMinimumCardinalityEnabled {
      qg = queryGraph
      indexOn("A", "foo")
    }.withLogicalPlanningContext { (cfg, ctx) =>
      val resultPlans = orLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

      val solveds = ctx.staticComponents.planningAttributes.solveds
      resultPlans should have size 1

      val resultQueryGraph = solveds.get(resultPlans.head.id).asSinglePlannerQuery.queryGraph
      resultQueryGraph.selections.flatPredicatesSet shouldEqual cfg.qg.selections.flatPredicatesSet

    }
  }

  test("should only generate a plan where the solved predicates are a subset of the input query graph") {
    val orLeafPlanner = orleafPlannerWithSubplans
    var numSuccessfulOrLeafsPlanned = 0

    forAll { (queryGraph: QueryGraph) =>
      new givenPlanWithMinimumCardinalityEnabled {
        qg = queryGraph
        indexOn("A", "foo")
        indexOn("B", "bar")
        relationshipIndexOn("R", "bar")
        relationshipIndexOn("T", "foo")
        relationshipIndexOn("T", "bar")
      }.withLogicalPlanningContext { (cfg, ctx) =>
        // when
        val resultPlans = orLeafPlanner(cfg.qg, InterestingOrderConfig.empty, ctx)

        val solveds = ctx.staticComponents.planningAttributes.solveds

        resultPlans.foreach { resultPlan =>
          val resultQueryGraph = solveds.get(resultPlan.id).asSinglePlannerQuery.queryGraph
          val unexpectedPredicates = resultQueryGraph.selections.predicates.diff(cfg.qg.selections.predicates)
          if (unexpectedPredicates.nonEmpty) {
            val prettyUnexpectedPredicates =
              unexpectedPredicates.map(predicate => expressionStringifier(predicate.expr)).mkString("[", ", ", "]")
            val prettyInputPredicates = cfg.qg.selections.predicates.map(predicate =>
              expressionStringifier(predicate.expr)
            ).mkString("[\n", ",\n ", "\n]")
            fail(
              s"""One or more predicates that were not in the original query-graph were marked as solved by the OrLeafPlanner
                 |  Unexpected predicates: $prettyUnexpectedPredicates
                 |  For Input Query: $prettyInputPredicates
                 |  Logical plan:
                 |$resultPlan""".stripMargin
            )
          }
        }
        if (resultPlans.nonEmpty)
          numSuccessfulOrLeafsPlanned += 1
      }
    }

    println(s"Total number of successful orleafs planned: $numSuccessfulOrLeafsPlanned")
  }

  private def orleafPlannerWithSubplans: OrLeafPlanner = {
    OrLeafPlanner(Seq(
      idSeekLeafPlanner(Set.empty),
      NodeIndexLeafPlanner(
        Seq(
          nodeIndexSeekPlanProvider,
          nodeIndexStringSearchScanPlanProvider,
          nodeIndexScanPlanProvider
        ),
        LeafPlanRestrictions.NoRestrictions
      ),
      RelationshipIndexLeafPlanner(
        Seq(
          RelationshipIndexScanPlanProvider,
          RelationshipIndexSeekPlanProvider,
          RelationshipIndexStringSearchScanPlanProvider
        ),
        LeafPlanRestrictions.NoRestrictions
      ),
      labelScanLeafPlanner(Set.empty),
      intersectionLabelScanLeafPlanner(Set.empty),
      unionLabelScanLeafPlanner(Set.empty),
      relationshipTypeScanLeafPlanner(Set.empty),
      unionRelationshipTypeScanLeafPlanner(Set.empty)
    ))
  }
}

object OrLeafPlannerTest {

  implicit val arbitraryQueryGraph: Arbitrary[QueryGraph] =
    Arbitrary(genQueryGraph)

  def genQueryGraph: Gen[QueryGraph] =
    for {
      relationshipTypes <- RelationshipTypes.genRelationshipTypes
      size <- Gen.size
      numberOfPredicates <- Gen.chooseNum(0, size.max(5))
      otherPredicates <- Gen.listOfN(numberOfPredicates, genPredicate)
    } yield mkQueryGraph(relationshipTypes.toList, otherPredicates)

  implicit val shrinkQueryGraph: Shrink[QueryGraph] =
    Shrink(queryGraph =>
      for {
        relationshipTypes <- Shrink.shrink(queryGraph.patternRelationships.head.types)
        otherPredicates <- Shrink.shrink(queryGraph.selections.flatPredicatesSet)
      } yield mkQueryGraph(relationshipTypes, otherPredicates)
    )

  private def mkQueryGraph(relationshipTypes: Seq[RelTypeName], otherPredicates: Iterable[Expression]): QueryGraph =
    QueryGraph(
      patternNodes = Set(var_a, var_b),
      patternRelationships =
        Set(PatternRelationship(var_r, (var_a, var_b), BOTH, relationshipTypes.toList, SimplePatternLength)),
      selections = Selections.from(otherPredicates)
    )

  implicit private val shrinkExpression: Shrink[Expression] =
    Shrink {
      case Not(expression) => Stream(expression)
      case Ands(expressions) =>
        Shrink.shrink(expressions).filter(_.nonEmpty).map(shrunk => Ands.create(shrunk))
      case Ors(expressions) =>
        Shrink.shrink(expressions).filter(_.nonEmpty).map(shrunk => Ors.create(shrunk))
      case _ => Stream.empty
    }

  private val var_a = Variable("a")(InputPosition.NONE)
  private val var_b = Variable("b")(InputPosition.NONE)
  private val var_r = Variable("r")(InputPosition.NONE)
  private val A = LabelName("A")(InputPosition.NONE)
  private val B = LabelName("B")(InputPosition.NONE)
  private val C = LabelName("C")(InputPosition.NONE)
  private val R = RelTypeName("R")(InputPosition.NONE)
  private val S = RelTypeName("R")(InputPosition.NONE)
  private val T = RelTypeName("R")(InputPosition.NONE)
  private val bar = PropertyKeyName("bar")(InputPosition.NONE)
  private val foo = PropertyKeyName("foo")(InputPosition.NONE)

  private def genPredicate: Gen[Expression] =
    Gen.recursive[Expression] { recursively =>
      Gen.sized { size =>
        if (size > 1)
          Gen.oneOf(
            genBasePredicate,
            negation(recursively),
            conjunction(recursively),
            disjunction(recursively)
          )
        else
          genBasePredicate
      }
    }

  private def negation(gen: Gen[Expression]): Gen[Not] =
    for {
      size <- Gen.size
      predicate <- Gen.resize(size - 1, gen)
    } yield Not(predicate)(InputPosition.NONE)

  private def conjunction(gen: Gen[Expression]): Gen[Expression] =
    expressions(gen) {
      exprs => Ands(exprs)(InputPosition.NONE)
    }

  private def disjunction(gen: Gen[Expression]): Gen[Expression] =
    expressions(gen) {
      exprs => Ors(exprs)(InputPosition.NONE)
    }

  private def expressions(gen: Gen[Expression])(f: List[Expression] => Expression): Gen[Expression] =
    for {
      size <- Gen.size
      numberOfPredicates <- Gen.chooseNum(1, size.min(1).max(5))
      predicates <- Gen.listOfN(numberOfPredicates, Gen.resize(size / 2, gen))
    } yield f(predicates)

  private def genBasePredicate: Gen[Expression] =
    Gen.oneOf(
      genHasTypes,
      genHasLabels,
      genPropertyPredicate,
      genBooleanLiteral
    )

  private def genHasTypes: Gen[HasTypes] =
    for {
      relationshipTypes <- RelationshipTypes.genNonEmptyRelationshipTypes
    } yield HasTypes(var_r, relationshipTypes.toList)(InputPosition.NONE)

  private object RelationshipTypes {
    private val all = Set(R, S, T)
    private val powerSet = all.subsets().toList
    private val nonEmptyPowerSet = powerSet.tail

    def genRelationshipTypes: Gen[Set[RelTypeName]] = Gen.oneOf(powerSet)
    def genNonEmptyRelationshipTypes: Gen[Set[RelTypeName]] = Gen.oneOf(nonEmptyPowerSet)
  }

  private def genHasLabels: Gen[HasLabels] =
    for {
      node <- Gen.oneOf(var_a, var_b)
      labelNames <- LabelNames.genLabelNames
    } yield HasLabels(node, labelNames.toList)(InputPosition.NONE)

  private object LabelNames {
    private val all = Set(A, B, C)
    private val nonEmptyPowerSet = all.subsets().toList.tail

    def genLabelNames: Gen[Set[LabelName]] = Gen.oneOf(nonEmptyPowerSet)
  }

  private def genPropertyPredicate: Gen[Expression] =
    for {
      variable <- Gen.oneOf(var_a, var_b, var_r)
      propertyKey <- Gen.oneOf(foo, bar)
      property = Property(variable, propertyKey)(InputPosition.NONE)
      propertyPredicate <- Gen.oneOf(
        Equals(property, fortyTwo)(InputPosition.NONE),
        IsNotNull(property)(InputPosition.NONE)
      )
    } yield propertyPredicate

  final private val fortyTwo = SignedDecimalIntegerLiteral("42")(InputPosition.NONE)

  private def genBooleanLiteral: Gen[BooleanLiteral] =
    Gen.oneOf(
      True()(InputPosition.NONE),
      False()(InputPosition.NONE)
    )
}
