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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps.CostComparisonListener
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality, CardinalityEstimation, Cost, PlannerQuery}
import org.neo4j.cypher.internal.v3_3.logical.plans._

import scala.collection.{immutable, mutable}

/** *
  * This class can listen to cost comparisons and report them back as normal rows. This is done by creating a fake plan
  * that looks something like this:
  *
  * UNWIND [{comparison: 0, planText: "plan", planCost: "costs", cost: 12, ...}] as col
  * RETURN col["comparison] as comparison
  *
  * This is the plan that actually gets run, and not the one that the user provided.
  *
  * WARNING: This is a debug feature, and as such, not tested as rigorously as other features are.
  * Only run this on a system that is not live after taking proper backups!
  */
class ReportCostComparisonsAsRows extends CostComparisonListener {

  private val rows = new mutable.ListBuffer[Row]()
  private val pos = InputPosition(0, 0, 0)
  private var comparisonCount = 0

  override def report[X](projector: X => LogicalPlan,
                         input: Iterable[X],
                         inputOrdering: Ordering[X],
                         context: LogicalPlanningContext): Unit = if (input.size > 1) {

    def stringTo(level: Int, plan: LogicalPlan): String = {
      def indent(level: Int, in: String): String = level match {
        case 0 => in
        case _ => System.lineSeparator() + "  " * level + in
      }

      val cost = context.cost(plan, context.input)
      val thisPlan = indent(level, s"${plan.getClass.getSimpleName} costs $cost cardinality ${plan.solved.estimatedCardinality}")
      val l = plan.lhs.map(p => stringTo(level + 1, p)).getOrElse("")
      val r = plan.rhs.map(p => stringTo(level + 1, p)).getOrElse("")
      thisPlan + l + r
    }

    val sortedPlans = input.toIndexedSeq.sorted(inputOrdering.reverse).map(projector)
    val winner = sortedPlans.last

    val theseRows: immutable.Seq[Row] = sortedPlans.map { plan =>
      val planText = plan.toString.replaceAll(System.lineSeparator(), System.lineSeparator())
      val planTextWithCosts = stringTo(0, plan).replaceAll(System.lineSeparator(), System.lineSeparator())
      val cost = context.cost(plan, context.input)
      val cardinality = plan.solved.estimatedCardinality

      Row(comparisonCount, planText, planTextWithCosts, cost, cardinality, winner == plan)
    }

    val divider = Row(comparisonCount, "*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-", "", Cost(0), Cardinality.SINGLE, false)

    rows ++= (divider +: theseRows)
    comparisonCount += 1
  }

  def addPlan(in: LogicalPlanState): LogicalPlanState = {
    val plan = asPlan()
    val newStatement = asStatement()

    var current: Option[LogicalPlan] = Some(plan)
    do {
      val thisPlan = current.get
      current = current.get.lhs
    } while (current.nonEmpty)

    in.copy(maybePeriodicCommit = Some(None), maybeLogicalPlan = Some(plan), maybeStatement = Some(newStatement))
  }

  private def asStatement(): Statement = {
    def ret(s: String) = AliasedReturnItem(varFor(s), varFor(s))(pos)

    val returnItems = Seq(
      ret("#"),
      ret("planText"),
      ret("planCost"),
      ret("cost"),
      ret("est cardinality"),
      ret("winner")
    )
    val returnClause = Return(distinct = false, ReturnItems(includeExisting = false, returnItems)(pos), None, None, None, None, Set.empty)(pos)
    Query(None, SingleQuery(Seq(returnClause))(pos))(pos)
  }

  private def varFor(s: String) = Variable(s)(pos)
  private def str(s: String): Expression = StringLiteral(s)(pos)
  private def int(i: Int): Expression = SignedDecimalIntegerLiteral(i.toString)(pos)
  private def dbl(d: Double): Expression = DecimalDoubleLiteral(d.toString)(pos)
  private def key(s: String): PropertyKeyName = PropertyKeyName(s)(pos)
  private def map(values: (String, Expression)*): MapExpression = MapExpression(values map { case (str, e) => key(str) -> e })(pos)
  private def solved = CardinalityEstimation.lift(PlannerQuery.empty, Cardinality.SINGLE)

  private def mapRowsToExpressions(comparisonId: Int, planText: String, planCosts: String, cost: Cost, cardinality: Cardinality, winner: Boolean): Seq[MapExpression] = {
    val planTestLines = planText.split(System.lineSeparator())
    val planCostLines = planCosts.split(System.lineSeparator())

    val details = planCostLines zip planTestLines map {
      case (planCost, planTxt) =>
        map(values =
          "comparison" -> int(comparisonId),
          "planDetails" -> str(planTxt),
          "planCosts" -> str(planCost)
        )
    }

    val summaryRow = map(
      "comparison" -> int(comparisonId),
      "planDetails" -> str(""),
      "planCosts" -> str(""),
      "cost" -> dbl(cost.gummyBears),
      "est cardinality" -> dbl(cardinality.amount),
      "winner" -> str(if (winner) "WON" else "LOST")
    )
    summaryRow +: details
  }

  private def asPlan(): LogicalPlan = {

    val maps: immutable.Seq[MapExpression] =
      rows.toIndexedSeq.reverse.flatMap(r => mapRowsToExpressions(r.comparisonId, r.planText, r.planCosts, r.cost, r.cardinality, r.winner))

    val plan: LogicalPlan =
      produceResult(
        project(
          unwindPlan(maps)))

    plan
  }

  private def produceResult(project: LogicalPlan): LogicalPlan =
    ProduceResult(Seq(
      "#",
      "planText",
      "planCost",
      "cost",
      "est cardinality",
      "winner"
    ), project)

  private def project(unwind: LogicalPlan): LogicalPlan = {
    val column = varFor("col")
    val comparisonE = Property(column, key("comparison"))(pos)
    val planTextE = Property(column, key("planDetails"))(pos)
    val planCostE = Property(column, key("planCosts"))(pos)
    val costE = Property(column, key("cost"))(pos)
    val cardinalityE = Property(column, key("est cardinality"))(pos)
    val winnerE = Property(column, key("winner"))(pos)

    val project: LogicalPlan = Projection(unwind, Map(
      "#" -> comparisonE,
      "planText" -> planTextE,
      "planCost" -> planCostE,
      "cost" -> costE,
      "est cardinality" -> cardinalityE,
      "winner" -> winnerE
    ))(solved)
    project
  }

  private def unwindPlan(maps: immutable.Seq[MapExpression]): LogicalPlan = {
    val expression = ListLiteral(maps)(pos)
    val unwind: LogicalPlan = UnwindCollection(SingleRow()(solved), "col", expression)(solved)
    unwind
  }

  case class Row(comparisonId: Int, planText: String, planCosts: String, cost: Cost, cardinality: Cardinality, winner: Boolean)

}
