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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.compiler.helpers.MapSupport.PowerMap
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.PlanReads
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.collectReads
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.PlanCreates
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.PlanSets
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.PlanWrites
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.collectWrites
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.logical.plans.AbstractLetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.util.InputPosition

object ReadsAndWritesFinder {

  /**
   * Stores information about what plans read which symbol (label, property, relationship type).
   * This is a generalization from just a map from a symbol to a collection of plans, because some operators can read unknown labels or properties.
   *
   * @param plansReadingConcreteSymbol map for finding plans which read a specific/concrete symbol
   * @param plansReadingUnknownSymbols all plans which read unknown symbols (of the given type)
   * @tparam T type of symbol, that is whether this stores labels, relationship types or properties
   */
  private[eager] case class ReadingPlansProvider[T <: SymbolicName](
    plansReadingConcreteSymbol: Map[T, Seq[LogicalPlan]] = Map.empty[T, Seq[LogicalPlan]],
    plansReadingUnknownSymbols: Seq[LogicalPlan] = Seq.empty
  ) {

    /**
     * @return all plans reading the given concrete symbol or unknown symbols.
     */
    def plansReadingSymbol(symbol: T): Seq[LogicalPlan] =
      plansReadingConcreteSymbol.getOrElse(symbol, Seq.empty) ++ plansReadingUnknownSymbols

    /**
     * @return all plans reading this kind of symbol.
     */
    def plansReadingAnySymbol(): Seq[LogicalPlan] =
      plansReadingConcreteSymbol.values.flatten.toSeq ++ plansReadingUnknownSymbols

    def withSymbolRead(symbol: T, plan: LogicalPlan): ReadingPlansProvider[T] = {
      val previousPlans = plansReadingConcreteSymbol.getOrElse(symbol, Seq.empty)
      copy(plansReadingConcreteSymbol.updated(symbol, previousPlans :+ plan))
    }

    def withUnknownSymbolsRead(plan: LogicalPlan): ReadingPlansProvider[T] = {
      copy(plansReadingUnknownSymbols = plansReadingUnknownSymbols :+ plan)
    }

    def ++(other: ReadingPlansProvider[T]): ReadingPlansProvider[T] =
      copy(
        plansReadingConcreteSymbol.fuse(other.plansReadingConcreteSymbol)(_ ++ _),
        plansReadingUnknownSymbols ++ other.plansReadingUnknownSymbols
      )
  }

  /**
   * Stores information about what plans write which property.
   * This is a generalization from just a map from a property to a collection of plans, because some operators can write unknown properties.
   *
   * @param plansWritingConcreteProperty A Map from the property name to the plans that write that property.
   * @param plansWritingUnknownProperty  all plans which write an unknown property
   */
  private[eager] case class PropertyWritingPlansProvider(
    plansWritingConcreteProperty: Map[PropertyKeyName, Seq[LogicalPlan]] = Map.empty,
    plansWritingUnknownProperty: Seq[LogicalPlan] = Seq.empty
  ) {

    def withPropertyWritten(property: PropertyKeyName, plan: LogicalPlan): PropertyWritingPlansProvider = {
      val previousPlans = plansWritingConcreteProperty.getOrElse(property, Seq.empty)
      copy(plansWritingConcreteProperty.updated(property, previousPlans :+ plan))
    }

    def withUnknownPropertyWritten(plan: LogicalPlan): PropertyWritingPlansProvider = {
      copy(plansWritingUnknownProperty = plansWritingUnknownProperty :+ plan)
    }

    def ++(other: PropertyWritingPlansProvider): PropertyWritingPlansProvider =
      copy(
        plansWritingConcreteProperty.fuse(other.plansWritingConcreteProperty)(_ ++ _),
        plansWritingUnknownProperty ++ other.plansWritingUnknownProperty
      )

    /**
     * @return A `Seq` of pairs that map a known `PropertyKeyName` (if `Some`) or an unknown `PropertyKeyName` (if `None`)
     *         to all plans writing that property.
     */
    def entries: Seq[(Option[PropertyKeyName], Seq[LogicalPlan])] =
      plansWritingConcreteProperty.toSeq.map {
        case (key, value) => (Some(key), value)
      } :+ (None, plansWritingUnknownProperty)
  }

  /**
   * This class groups expression that all filter on the same variable.
   *
   * @param plansThatIntroduceVariable all plans that introduce the variable that the expression filters on.
   * @param expression                 an expression of all predicates related to one variable.
   */
  case class FilterExpressions(
    plansThatIntroduceVariable: Seq[LogicalPlan],
    expression: Expression = Ands(Seq.empty)(InputPosition.NONE)
  ) {

    def withAddedExpression(newExp: Expression): FilterExpressions = {
      val compositeExpression = expression match {
        case Ands(SetExtractor()) => newExp
        case Ors(SetExtractor())  => newExp
        case Ands(exprs)          => Ands(exprs + newExp)(InputPosition.NONE)
        case Ors(exprs)           => Ors(exprs + newExp)(InputPosition.NONE)
        case _                    => Ands(Seq(expression, newExp))(InputPosition.NONE)
      }
      copy(expression = compositeExpression)
    }

    def ++(other: FilterExpressions, mergePlan: LogicalBinaryPlan): FilterExpressions = {
      val allPlans = this.plansThatIntroduceVariable ++ other.plansThatIntroduceVariable

      val lhs = this
      val rhs = other
      val compositeExpression = mergePlan match {
        case _: Union | _: OrderedUnion =>
          // Union expresses OR
          Ors(Seq(lhs.expression, rhs.expression))(InputPosition.NONE)
        case _: NodeHashJoin | _: ValueHashJoin | _: AssertSameNode =>
          // Joins express AND
          // Let's use withAddedExpression to avoid nesting Ands
          lhs.withAddedExpression(rhs.expression).expression
        case _: LeftOuterHashJoin =>
          // RHS predicates might not be applied if the rows are not matched in the hash table
          lhs.expression
        case _: RightOuterHashJoin =>
          // LHS predicates might not be applied if the rows are not matched in the hash table
          rhs.expression
        case _: CartesianProduct =>
          // If under an apply, the branches can share an argument variable and then we want to combine the predicates,
          // but the rhs includes the lhs already, so no need to merge.
          rhs.expression
        case _: AbstractLetSelectOrSemiApply |
          _: AbstractSelectOrSemiApply |
          _: AbstractLetSemiApply =>
          // These SemiApplyPlans express predicates for exists with an OR between predicates,
          // and then we must exclude the RHS predicates to be safe, e.g.
          // MATCH /* unstable */ (a:A) WHERE exists { (a:B) } OR true CREATE (:A)
          // should be Eager.
          lhs.expression
        case _: AntiSemiApply =>
          // Something like lhs.expression AND NOT rhs.expression would be correct,
          // but given the way LogicalPlans.foldPlan works, rhs.expression actually contains lhs.expression.
          // A safe, but too conservative solution is to simply exclude the RHS predicates.
          lhs.expression
        case _: RollUpApply |
          _: SubqueryForeach |
          _: TransactionForeach |
          _: ForeachApply |
          _: AntiConditionalApply =>
          // These plans are used for subqueries and sometimes yield lhs rows more or less unchanged.
          // So any rhs predicates on already defined variables must not be considered.
          lhs.expression
        case ApplyPlan(_, applyRhs) if applyRhs.folder.treeFindByClass[Optional].nonEmpty =>
          // RHS predicates might not be applied if the rows are filtered out by Optional
          lhs.expression
        case _: Apply |
          _: SemiApply =>
          // These ApplyPlans simply combine the predicates,
          // but the rhs includes the lhs already, so no need to merge.
          rhs.expression
        case _: ApplyPlan =>
          // For any other apply plans we exclude the RHS predicates, which is the safe option.
          lhs.expression
      }

      copy(
        plansThatIntroduceVariable = allPlans,
        expression = compositeExpression
      )
    }
  }

  /**
   * An accumulator of reads in the logical plan tree.
   *
   * @param readProperties a provider to find out which plans read which properties.
   * @param readLabels a provider to find out which plans read which labels.
   * @param filterExpressions for each variable the expressions that filter on that variable.
   * @param allNodeReadPlans all AllNodesScan plans
   */
  private[eager] case class Reads(
    readProperties: ReadingPlansProvider[PropertyKeyName] = ReadingPlansProvider(),
    readLabels: ReadingPlansProvider[LabelName] = ReadingPlansProvider(),
    filterExpressions: Map[LogicalVariable, FilterExpressions] = Map.empty,
    allNodeReadPlans: Seq[LogicalPlan] = Seq.empty
  ) {

    /**
     * @param property if `Some(prop)` look for plans that could potentially read that property,
     *                 if `None` look for plans that read any property.
     * @return all plans that read the given property.
     */
    def plansReadingProperty(property: Option[PropertyKeyName]): Seq[LogicalPlan] =
      property match {
        case Some(property) => readProperties.plansReadingSymbol(property)
        case None           => readProperties.plansReadingAnySymbol()
      }

    /**
     * @return all plans that could read the given label.
     */
    def plansReadingLabel(label: LabelName): Seq[LogicalPlan] =
      readLabels.plansReadingSymbol(label)

    def withPropertyRead(property: PropertyKeyName, plan: LogicalPlan): Reads =
      copy(readProperties = readProperties.withSymbolRead(property, plan))

    def withUnknownPropertiesRead(plan: LogicalPlan): Reads =
      copy(readProperties = readProperties.withUnknownSymbolsRead(plan))

    def withLabelRead(label: LabelName, plan: LogicalPlan): Reads = {
      copy(readLabels = readLabels.withSymbolRead(label, plan))
    }

    def withAddedFilterExpression(
      variable: LogicalVariable,
      plan: LogicalPlan,
      filterExpression: Expression
    ): Reads = {
      val newExpressions =
        filterExpressions.getOrElse(variable, FilterExpressions(Seq(plan))).withAddedExpression(
          filterExpression
        )
      copy(filterExpressions = filterExpressions + (variable -> newExpressions))
    }

    def withUnknownLabelsRead(plan: LogicalPlan): Reads =
      copy(readLabels = readLabels.withUnknownSymbolsRead(plan))

    def withAllNodesRead(plan: LogicalPlan): Reads =
      copy(allNodeReadPlans = allNodeReadPlans :+ plan)

    def includePlanReads(plan: LogicalPlan, planReads: PlanReads): Reads = {
      Option(this)
        .map(acc => planReads.readProperties.foldLeft(acc)(_.withPropertyRead(_, plan)))
        .map(acc => planReads.readLabels.foldLeft(acc)(_.withLabelRead(_, plan)))
        .map(acc =>
          planReads.filterExpressions.foldLeft(acc) {
            case (acc, (variable, expressions)) =>
              expressions.foldLeft(acc)(_.withAddedFilterExpression(variable, plan, _))
          }
        )
        .map(acc => if (planReads.readsUnknownLabels) acc.withUnknownLabelsRead(plan) else acc)
        .map(acc => if (planReads.readsUnknownProperties) acc.withUnknownPropertiesRead(plan) else acc)
        .map(acc => if (planReads.readsAllNodes) acc.withAllNodesRead(plan) else acc)
        .get
    }

    /**
     * Returns a copy of this class, except that the [[FilterExpressions]] from the other plan are merged in
     * as if it was invoked like `other ++ (this, mergePlan)`.
     */
    def mergeFilterExpressions(other: Reads, mergePlan: LogicalBinaryPlan): Reads = {
      copy(
        filterExpressions = other.filterExpressions.fuse(this.filterExpressions)(_ ++ (_, mergePlan))
      )
    }

    def ++(other: Reads, mergePlan: LogicalBinaryPlan): Reads = {
      copy(
        readProperties = this.readProperties ++ other.readProperties,
        readLabels = this.readLabels ++ other.readLabels,
        filterExpressions = this.filterExpressions.fuse(other.filterExpressions)(_ ++ (_, mergePlan)),
        allNodeReadPlans = this.allNodeReadPlans ++ other.allNodeReadPlans
      )
    }
  }

  /**
   * An accumulator of SETs in the logical plan tree.
   *
   * @param writtenProperties a provider to find out which plans set which properties.
   * @param writtenLabels     for each label, all the plans that set that label.
   */
  private[eager] case class Sets(
    writtenProperties: PropertyWritingPlansProvider = PropertyWritingPlansProvider(),
    writtenLabels: Map[LabelName, Seq[LogicalPlan]] = Map.empty
  ) {

    def withPropertyWritten(property: PropertyKeyName, plan: LogicalPlan): Sets =
      copy(writtenProperties = writtenProperties.withPropertyWritten(property, plan))

    def withUnknownPropertyWritten(plan: LogicalPlan): Sets =
      copy(writtenProperties = writtenProperties.withUnknownPropertyWritten(plan))

    def withLabelWritten(label: LabelName, plan: LogicalPlan): Sets = {
      val wL = writtenLabels.getOrElse(label, Seq.empty)
      copy(writtenLabels = writtenLabels.updated(label, wL :+ plan))
    }

    def includePlanSets(plan: LogicalPlan, planSets: PlanSets): Sets = {
      Option(this)
        .map(acc => planSets.writtenProperties.foldLeft(acc)(_.withPropertyWritten(_, plan)))
        .map(acc => if (planSets.writesUnknownProperties) acc.withUnknownPropertyWritten(plan) else acc)
        .map(acc => planSets.writtenLabels.foldLeft(acc)(_.withLabelWritten(_, plan)))
        .get
    }

    def ++(other: Sets): Sets = {
      copy(
        writtenProperties = this.writtenProperties ++ other.writtenProperties,
        writtenLabels = this.writtenLabels.fuse(other.writtenLabels)(_ ++ _)
      )
    }
  }

  /**
   * An accumulator of CREATEs in the logical plan tree.
   *
   * @param writtenProperties          a provider to find out which plans create which properties.
   * @param writtenLabels              for each plan, the labels that it creates. We cannot group this by label instead,
   *                                   because we need to view the composite label expressions when checking against
   *                                   filterExpressions.
   * @param plansThatCreateNodes       all plans that create nodes
   * @param filterExpressionsSnapshots for each plan (that will need to look at that later), a snapshot of the current filterExpressions.
   */
  private[eager] case class Creates(
    writtenProperties: PropertyWritingPlansProvider = PropertyWritingPlansProvider(),
    writtenLabels: Map[LogicalPlan, Seq[LabelName]] = Map.empty,
    plansThatCreateNodes: Seq[LogicalPlan] = Seq.empty,
    filterExpressionsSnapshots: Map[LogicalPlan, Map[LogicalVariable, FilterExpressions]] = Map.empty
  ) {

    def withPropertyWritten(property: PropertyKeyName, plan: LogicalPlan): Creates =
      copy(writtenProperties = writtenProperties.withPropertyWritten(property, plan))

    def withUnknownPropertyWritten(plan: LogicalPlan): Creates =
      copy(writtenProperties = writtenProperties.withUnknownPropertyWritten(plan))

    /**
     * Save that `plan` writes `label`.
     * Since CREATE plans need to look for conflicts in the _current_ filterExpressions, we save a snapshot of the current filterExpressions,
     * associated with the CREATE plan. If we were to include all filterExpressions, we might think that Eager is not needed, even though it is.
     * See test "inserts eager between Create and NodeByLabelScan if label overlap, and other label in Filter after create".
     */
    def withLabelWritten(
      label: LabelName,
      plan: LogicalPlan,
      filterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions]
    ): Creates = {
      val prevLabels = writtenLabels.getOrElse(plan, Seq.empty)
      copy(
        writtenLabels = writtenLabels.updated(plan, prevLabels :+ label),
        filterExpressionsSnapshots = filterExpressionsSnapshots + (plan -> filterExpressionsSnapshot)
      )
    }

    def withNodesCreated(plan: LogicalPlan): Creates =
      copy(plansThatCreateNodes = plansThatCreateNodes :+ plan)

    def includePlanCreates(
      plan: LogicalPlan,
      planCreates: PlanCreates,
      filterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions]
    ): Creates = {
      Option(this)
        .map(acc => planCreates.writtenProperties.foldLeft(acc)(_.withPropertyWritten(_, plan)))
        .map(acc => if (planCreates.writesUnknownProperties) acc.withUnknownPropertyWritten(plan) else acc)
        .map(acc => planCreates.writtenLabels.foldLeft(acc)(_.withLabelWritten(_, plan, filterExpressionsSnapshot)))
        .map(acc => if (planCreates.createsNodes) acc.withNodesCreated(plan) else acc)
        .get
    }

    def ++(other: Creates): Creates = {
      copy(
        writtenProperties = this.writtenProperties ++ other.writtenProperties,
        writtenLabels = this.writtenLabels.fuse(other.writtenLabels)(_ ++ _),
        plansThatCreateNodes = this.plansThatCreateNodes ++ other.plansThatCreateNodes,
        filterExpressionsSnapshots = this.filterExpressionsSnapshots ++ other.filterExpressionsSnapshots
      )
    }
  }

  /**
   * An accumulator of writes  in the logical plan tree.
   */
  private[eager] case class Writes(
    sets: Sets = Sets(),
    creates: Creates = Creates()
  ) {

    def withSets(sets: Sets): Writes = copy(sets = sets)

    def withCreates(creates: Creates): Writes = copy(creates = creates)

    def includePlanWrites(
      plan: LogicalPlan,
      planWrites: PlanWrites,
      filterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions]
    ): Writes = {
      Option(this)
        .map(acc => acc.withSets(acc.sets.includePlanSets(plan, planWrites.sets)))
        .map(acc =>
          acc.withCreates(acc.creates.includePlanCreates(plan, planWrites.creates, filterExpressionsSnapshot))
        )
        .get
    }

    def ++(other: Writes): Writes = {
      copy(
        sets = this.sets ++ other.sets,
        creates = this.creates ++ other.creates
      )
    }
  }

  /**
   * Reads and writes of multiple plans.
   * The Seqs nested in this class may contain duplicates. These are filtered out later in [[ConflictFinder]].
   */
  private[eager] case class ReadsAndWrites(
    reads: Reads = Reads(),
    writes: Writes = Writes()
  ) {

    def withReads(reads: Reads): ReadsAndWrites = copy(reads = reads)

    def withWrites(writes: Writes): ReadsAndWrites = copy(writes = writes)

    /**
     * Returns a copy of this class, except that the [[FilterExpressions]] from the other plan are merged in
     * as if it was invoked like `other ++ (this, mergePlan)`.
     */
    def mergeFilterExpressions(other: ReadsAndWrites, mergePlan: LogicalBinaryPlan): ReadsAndWrites = {
      copy(
        reads = this.reads.mergeFilterExpressions(other.reads, mergePlan)
      )
    }

    def ++(other: ReadsAndWrites, mergePlan: LogicalBinaryPlan): ReadsAndWrites = {
      copy(
        reads = this.reads ++ (other.reads, mergePlan),
        writes = this.writes ++ other.writes
      )
    }
  }

  /**
   * Traverse plan in execution order and remember all reads and writes.
   */
  private[eager] def collectReadsAndWrites(wholePlan: LogicalPlan): ReadsAndWrites = {
    def processPlan(acc: ReadsAndWrites, plan: LogicalPlan): ReadsAndWrites = {
      val planReads = collectReads(plan)
      val planWrites = collectWrites(plan)

      Option(acc)
        .map(acc => acc.withReads(acc.reads.includePlanReads(plan, planReads)))
        .map {
          val filterExpressionsSnapshot = acc.reads.filterExpressions
          acc => acc.withWrites(acc.writes.includePlanWrites(plan, planWrites, filterExpressionsSnapshot))
        }
        .get
    }

    LogicalPlans.foldPlan(ReadsAndWrites())(
      wholePlan,
      (acc, plan) => processPlan(acc, plan),
      (lhsAcc, rhsAcc, plan) =>
        plan match {
          case _: ApplyPlan =>
            // The RHS was initialized with the LHS in LogicalPlans.foldPlan,
            // but lhsAcc filterExpressions still need to be merged into rhsAcc by some rules defined FilterExpressions.
            val acc = rhsAcc.mergeFilterExpressions(lhsAcc, plan)
            processPlan(acc, plan)
          case _ =>
            // We need to merge LHS and RHS
            val acc = lhsAcc ++ (rhsAcc, plan)
            processPlan(acc, plan)
        }
    )
  }
}
