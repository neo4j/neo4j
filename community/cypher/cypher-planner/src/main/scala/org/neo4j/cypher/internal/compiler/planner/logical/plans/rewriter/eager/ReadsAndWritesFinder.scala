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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.ChildrenIds
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.AccessedLabel
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.AccessedProperty
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.PlanReads
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.ReadFinder.collectReads
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.CreatedNode
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.CreatedRelationship
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.PlanCreates
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.WriteFinder.PlanDeletes
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
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.logical.plans.AbstractLetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.ApplyPlan
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

object ReadsAndWritesFinder {

  /**
   * A plan that accesses something (e.g. a label or property), optionally through an accessor variable, if available.
   */
  case class PlanWithAccessor(plan: LogicalPlan, accessor: Option[LogicalVariable])

  /**
   * Stores information about what plans read which symbol (label, property, relationship type).
   * This is a generalization from just a map from a symbol to a collection of plans, because some operators can read unknown labels or properties.
   *
   * @param plansReadingConcreteSymbol map for finding plans which read a specific/concrete symbol
   * @param plansReadingUnknownSymbols all plans which read unknown symbols (of the given type)
   * @tparam T type of symbol, that is whether this stores labels, relationship types or properties
   */
  private[eager] case class ReadingPlansProvider[T <: SymbolicName](
    plansReadingConcreteSymbol: Map[T, Seq[PlanWithAccessor]] = Map.empty[T, Seq[PlanWithAccessor]],
    plansReadingUnknownSymbols: Seq[PlanWithAccessor] = Seq.empty
  ) {

    /**
     * @return all plans reading the given concrete symbol or unknown symbols.
     */
    def plansReadingSymbol(symbol: T): Iterator[PlanWithAccessor] =
      plansReadingConcreteSymbol.getOrElse(symbol, Seq.empty).iterator ++ plansReadingUnknownSymbols.iterator

    /**
     * @return all plans reading this kind of symbol.
     */
    def plansReadingAnySymbol(): Iterator[PlanWithAccessor] =
      plansReadingConcreteSymbol.values.iterator.flatten ++ plansReadingUnknownSymbols.iterator

    def withSymbolRead(symbol: T, planWithAccessor: PlanWithAccessor): ReadingPlansProvider[T] = {
      val previousPlans = plansReadingConcreteSymbol.getOrElse(symbol, Seq.empty)
      copy(plansReadingConcreteSymbol.updated(symbol, previousPlans :+ planWithAccessor))
    }

    def withUnknownSymbolsRead(planWithAccessor: PlanWithAccessor): ReadingPlansProvider[T] = {
      copy(plansReadingUnknownSymbols = plansReadingUnknownSymbols :+ planWithAccessor)
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
   * @param plansWritingConcreteNodeProperty A Map from the Node property name to the plans that write that property.
   * @param plansWritingUnknownNodeProperty  all plans which write an unknown Node property
   * @param plansWritingConcreteRelProperty A Map from the Relationship property name to the plans that write that property.
   * @param plansWritingUnknownRelProperty all plans which write an unknown Relationship property
   */
  private[eager] case class PropertyWritingPlansProvider(
    plansWritingConcreteNodeProperty: Map[PropertyKeyName, Seq[PlanWithAccessor]] = Map.empty,
    plansWritingConcreteRelProperty: Map[PropertyKeyName, Seq[PlanWithAccessor]] = Map.empty,
    plansWritingUnknownNodeProperty: Seq[PlanWithAccessor] = Seq.empty,
    plansWritingUnknownRelProperty: Seq[PlanWithAccessor] = Seq.empty
  ) {

    def withNodePropertyWritten(
      property: PropertyKeyName,
      planWithAccessor: PlanWithAccessor
    ): PropertyWritingPlansProvider = {
      val previousPlans = plansWritingConcreteNodeProperty.getOrElse(property, Seq.empty)
      copy(plansWritingConcreteNodeProperty =
        plansWritingConcreteNodeProperty.updated(property, previousPlans :+ planWithAccessor)
      )
    }

    def withRelPropertyWritten(
      property: PropertyKeyName,
      planWithAccessor: PlanWithAccessor
    ): PropertyWritingPlansProvider = {
      val previousPlans = plansWritingConcreteRelProperty.getOrElse(property, Seq.empty)
      copy(plansWritingConcreteRelProperty =
        plansWritingConcreteRelProperty.updated(property, previousPlans :+ planWithAccessor)
      )
    }

    def withUnknownNodePropertyWritten(planWithAccessor: PlanWithAccessor): PropertyWritingPlansProvider = {
      copy(plansWritingUnknownNodeProperty = plansWritingUnknownNodeProperty :+ planWithAccessor)
    }

    def withUnknownRelPropertyWritten(planWithAccessor: PlanWithAccessor): PropertyWritingPlansProvider = {
      copy(plansWritingUnknownRelProperty = plansWritingUnknownRelProperty :+ planWithAccessor)
    }

    def ++(other: PropertyWritingPlansProvider): PropertyWritingPlansProvider =
      copy(
        plansWritingConcreteNodeProperty.fuse(other.plansWritingConcreteNodeProperty)(_ ++ _),
        plansWritingConcreteRelProperty.fuse(other.plansWritingConcreteRelProperty)(_ ++ _),
        plansWritingUnknownNodeProperty ++ other.plansWritingUnknownNodeProperty,
        plansWritingUnknownRelProperty ++ other.plansWritingUnknownRelProperty
      )

    /**
     * @return An `Iterator` of pairs that map a known nodes `PropertyKeyName` (if `Some`) or an unknown nodes `PropertyKeyName` (if `None`)
     *         to all plans writing that property.
     */
    def nodeEntries: Iterator[(Option[PropertyKeyName], Seq[PlanWithAccessor])] =
      plansWritingConcreteNodeProperty.iterator.map {
        case (key, value) => (Some(key), value)
      } ++ Iterator((None, plansWritingUnknownNodeProperty))

    /**
     * @return An `Iterator` of pairs that map a known relationships `PropertyKeyName` (if `Some`) or an unknown relationships `PropertyKeyName` (if `None`)
     *         to all plans writing that property.
     */
    def relEntries: Iterator[(Option[PropertyKeyName], Seq[PlanWithAccessor])] =
      plansWritingConcreteRelProperty.iterator.map {
        case (key, value) => (Some(key), value)
      } ++ Iterator((None, plansWritingUnknownRelProperty))
  }

  /**
   * This class groups expression that all filter on the same variable.
   *
   * @param plansThatIntroduceVariable all plans that introduce the variable that the expression filters on.
   * @param expression                 an expression of all predicates related to one variable.
   */
  case class FilterExpressions(
    plansThatIntroduceVariable: Set[Ref[LogicalPlan]],
    expression: Expression = Ands(Seq.empty)(InputPosition.NONE)
  ) {

    def withAddedExpression(newExp: Expression): FilterExpressions = {
      val compositeExpression = expression match {
        case Ands(SetExtractor()) => newExp
        case Ands(exprs)          => Ands(exprs + newExp)(InputPosition.NONE)
        case _                    => Ands(Seq(expression, newExp))(InputPosition.NONE)
      }
      copy(expression = compositeExpression)
    }

    def mergeWith(
      other: FilterExpressions,
      mergePlan: LogicalBinaryPlan
    )(implicit containsOptional: Ref[LogicalPlan] => Boolean): FilterExpressions = {
      val allPlans = this.plansThatIntroduceVariable ++ other.plansThatIntroduceVariable

      val lhs = this
      val rhs = other
      val compositeExpression = mergePlan match {
        case _: Union | _: OrderedUnion =>
          // Union expresses OR
          Ors(Seq(lhs.expression, rhs.expression))(InputPosition.NONE)
        case _: NodeHashJoin | _: ValueHashJoin | _: AssertSameNode | _: AssertSameRelationship =>
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
        case _: RepeatOptions =>
          // The two branches should have the exact same reads, we can simply take the lhs
          lhs.expression
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
        case ApplyPlan(_, applyRhs) if containsOptional(Ref(applyRhs)) =>
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
   * A variable can either be introduced by one (or more) leaf plans, or by some inner plan.
   * For DELETE conflicts, only the predicates from leaf plans can be included.
   *
   * This trait tracks the plans that introduce a variable (potentially multiple in the case of UNION),
   * together with the predicates solved in the plans.
   * It also tracks the plans that last references the variable (potentially multiple in the case of UNION).
   *
   * The variable itself is not tracked here, but is the key in [[Reads.possibleNodeDeleteConflictPlans]].
   *
   * @param plansThatIntroduceVariable a list of plans that introduce the variable
   *                                   or have a filter on the variable. The plan is bundled with
   *                                   a list of predicates that all depend on the variable.
   * @param plansThatReferenceVariable the plans that reference the variable. These are the read plans that must be used to define
   *                                   the Conflict on. We cannot only use an earlier plan (e.g. where the variable was introduced), like in
   *                                   CREATE/MATCH conflicts, because evaluating any expression on a deleted node might crash.
   */
  case class PossibleDeleteConflictPlans(
    plansThatIntroduceVariable: Seq[PlanThatIntroducesVariable],
    plansThatReferenceVariable: Seq[LogicalPlan]
  ) {

    def ++(other: PossibleDeleteConflictPlans): PossibleDeleteConflictPlans = {
      PossibleDeleteConflictPlans(
        this.plansThatIntroduceVariable ++ other.plansThatIntroduceVariable,
        this.plansThatReferenceVariable ++ other.plansThatReferenceVariable
      )
    }
  }

  /**
   * A plan that introduces a variable together with the predicates for that variable solved by that plan.
   */
  case class PlanThatIntroducesVariable(plan: LogicalPlan, predicates: Seq[Expression])

  /**
   * An accumulator of reads in the logical plan tree.
   *
   * @param readNodeProperties              a provider to find out which plans read which node properties.
   * @param readLabels                      a provider to find out which plans read which labels.
   * @param nodeFilterExpressions           for each node variable the expressions that filter on that variable.
   *                                        This also tracks if a variable is introduced by a plan.
   *                                        If a variable is introduced by a plan, and no predicates are applied on that variable,
   *                                        it is still present as a key in this map.
   * @param possibleNodeDeleteConflictPlans for each node variable, the [[PossibleDeleteConflictPlans]]
   * @param readRelProperties               a provider to find out which plans read which relationship properties.
   * @param relationshipFilterExpressions   for each relationship variable the expressions that filter on that variable.
   *                                        This also tracks if a variable is introduced by a plan.
   *                                        If a variable is introduced by a plan, and no predicates are applied on that variable,
   *                                        it is still present as a key in this map.
   * @param possibleRelDeleteConflictPlans  for each relationship variable, the [[PossibleDeleteConflictPlans]]
   */
  private[eager] case class Reads(
    readNodeProperties: ReadingPlansProvider[PropertyKeyName] = ReadingPlansProvider(),
    readLabels: ReadingPlansProvider[LabelName] = ReadingPlansProvider(),
    nodeFilterExpressions: Map[LogicalVariable, FilterExpressions] = Map.empty,
    possibleNodeDeleteConflictPlans: Map[LogicalVariable, PossibleDeleteConflictPlans] = Map.empty,
    readRelProperties: ReadingPlansProvider[PropertyKeyName] = ReadingPlansProvider(),
    relationshipFilterExpressions: Map[LogicalVariable, FilterExpressions] = Map.empty,
    possibleRelDeleteConflictPlans: Map[LogicalVariable, PossibleDeleteConflictPlans] = Map.empty,
    callInTxPlans: Set[LogicalPlan] = Set.empty
  ) {

    /**
     * @param property if `Some(prop)` look for plans that could potentially read that node property,
     *                 if `None` look for plans that read any property.
     * @return all plans that read the given node property.
     */
    def plansReadingNodeProperty(property: Option[PropertyKeyName]): Iterator[PlanWithAccessor] =
      property match {
        case Some(property) => readNodeProperties.plansReadingSymbol(property)
        case None           => readNodeProperties.plansReadingAnySymbol()
      }

    /**
     * @param property if `Some(prop)` look for plans that could potentially read that relationship property,
     *                 if `None` look for plans that read any node property.
     * @return all plans that read the given property.
     */
    def plansReadingRelProperty(property: Option[PropertyKeyName]): Iterator[PlanWithAccessor] =
      property match {
        case Some(property) => readRelProperties.plansReadingSymbol(property)
        case None           => readRelProperties.plansReadingAnySymbol()
      }

    /**
     * @return all plans that could read the given label.
     */
    def plansReadingLabel(label: LabelName): Iterator[PlanWithAccessor] =
      readLabels.plansReadingSymbol(label)

    def withNodePropertyRead(accessedProperty: AccessedProperty, plan: LogicalPlan): Reads =
      copy(readNodeProperties =
        readNodeProperties.withSymbolRead(accessedProperty.property, PlanWithAccessor(plan, accessedProperty.accessor))
      )

    def withRelPropertyRead(accessedProperty: AccessedProperty, plan: LogicalPlan): Reads =
      copy(readRelProperties =
        readRelProperties.withSymbolRead(accessedProperty.property, PlanWithAccessor(plan, accessedProperty.accessor))
      )

    def withUnknownNodePropertiesRead(plan: LogicalPlan, accessor: Option[LogicalVariable]): Reads =
      copy(readNodeProperties = readNodeProperties.withUnknownSymbolsRead(PlanWithAccessor(plan, accessor)))

    def withUnknownRelPropertiesRead(plan: LogicalPlan, accessor: Option[LogicalVariable]): Reads =
      copy(readRelProperties = readRelProperties.withUnknownSymbolsRead(PlanWithAccessor(plan, accessor)))

    def withLabelRead(accessedLabel: AccessedLabel, plan: LogicalPlan): Reads = {
      copy(readLabels = readLabels.withSymbolRead(accessedLabel.label, PlanWithAccessor(plan, accessedLabel.accessor)))
    }

    def withCallInTx(plan: LogicalPlan): Reads = {
      copy(callInTxPlans = callInTxPlans + plan)
    }

    /**
     * Save that the plan introduces a node variable.
     * This is done by saving an empty filter expressions.
     */
    def withIntroducedNodeVariable(
      variable: LogicalVariable,
      plan: LogicalPlan
    ): Reads = {
      val newExpressions = nodeFilterExpressions.getOrElse(variable, FilterExpressions(Set(Ref(plan))))
      copy(nodeFilterExpressions = nodeFilterExpressions + (variable -> newExpressions))
    }

    /**
     * Save that the plan introduces a relationship variable.
     * This is done by saving an empty filter expressions.
     */
    def withIntroducedRelationshipVariable(
      variable: LogicalVariable,
      plan: LogicalPlan
    ): Reads = {
      val newExpressions = relationshipFilterExpressions.getOrElse(variable, FilterExpressions(Set(Ref(plan))))
      copy(relationshipFilterExpressions = relationshipFilterExpressions + (variable -> newExpressions))
    }

    def withAddedNodeFilterExpression(
      variable: LogicalVariable,
      plan: LogicalPlan,
      filterExpression: Expression
    ): Reads = {
      val newExpressions =
        nodeFilterExpressions.getOrElse(variable, FilterExpressions(Set(Ref(plan)))).withAddedExpression(
          filterExpression
        )
      copy(nodeFilterExpressions = nodeFilterExpressions + (variable -> newExpressions))
    }

    def withAddedRelationshipFilterExpression(
      variable: LogicalVariable,
      plan: LogicalPlan,
      filterExpression: Expression
    ): Reads = {
      val newExpressions =
        relationshipFilterExpressions.getOrElse(variable, FilterExpressions(Set(Ref(plan)))).withAddedExpression(
          filterExpression
        )
      copy(relationshipFilterExpressions = relationshipFilterExpressions + (variable -> newExpressions))
    }

    def withUnknownLabelsRead(plan: LogicalPlan, maybeVar: Option[LogicalVariable]): Reads =
      copy(readLabels = readLabels.withUnknownSymbolsRead(PlanWithAccessor(plan, maybeVar)))

    private def variableReferencesInPlan(
      plan: LogicalPlan,
      expressions: Seq[Expression],
      variable: LogicalVariable,
      possibleDeleteConflictPlans: Map[LogicalVariable, PossibleDeleteConflictPlans]
    ): Map[LogicalVariable, PossibleDeleteConflictPlans] = {
      val prev = possibleDeleteConflictPlans.getOrElse(variable, PossibleDeleteConflictPlans(Seq.empty, Seq.empty))
      val plansThatIntroduceVariable =
        if (prev.plansThatIntroduceVariable.isEmpty) {
          // This plan introduces the variable.

          // We should take predicates on leaf plans into account.
          val expressionsToInclude = plan match {
            case _: LogicalLeafPlan => expressions
            case _                  => Seq.empty[Expression]
          }
          Seq(PlanThatIntroducesVariable(plan, expressionsToInclude))
        } else {
          prev.plansThatIntroduceVariable
        }

      val plansThatReferenceVariable = prev.plansThatReferenceVariable :+ plan

      possibleDeleteConflictPlans
        .updated(variable, PossibleDeleteConflictPlans(plansThatIntroduceVariable, plansThatReferenceVariable))
    }

    /**
     * Update [[PossibleDeleteConflictPlans]].
     * This should be called if `plan` references `variable`.
     *
     * If `plan` filters on `variable`, the `expressions` should be all filters on `variable`.
     *
     * @param expressions all expressions in `plan` that filter on `variable`.
     */
    def withUpdatedPossibleDeleteNodeConflictPlans(
      plan: LogicalPlan,
      variable: LogicalVariable,
      expressions: Seq[Expression]
    ): Reads = {
      copy(possibleNodeDeleteConflictPlans =
        variableReferencesInPlan(plan, expressions, variable, possibleNodeDeleteConflictPlans)
      )
    }

    /**
     * Update [[PossibleDeleteConflictPlans]].
     * This should be called if `plan` references `variable`.
     *
     * If `plan` filters on `variable`, the `expressions` should be all filters on `variable`.
     *
     * @param expressions all expressions in `plan` that filter on `variable`.
     */
    def withUpdatedPossibleRelDeleteConflictPlans(
      plan: LogicalPlan,
      variable: LogicalVariable,
      expressions: Seq[Expression]
    ): Reads = {
      copy(possibleRelDeleteConflictPlans =
        variableReferencesInPlan(plan, expressions, variable, possibleRelDeleteConflictPlans)
      )
    }

    /**
     * Update [[PossibleDeleteConflictPlans.plansThatReferenceVariable]].
     * This should be called if a plan references a node variable.
     */
    def updatePlansThatReferenceNodeVariable(plan: LogicalPlan, variable: LogicalVariable): Reads = {
      val prev = possibleNodeDeleteConflictPlans.getOrElse(variable, PossibleDeleteConflictPlans(Seq.empty, Seq.empty))
      val next = prev.copy(plansThatReferenceVariable = prev.plansThatReferenceVariable :+ plan)
      copy(possibleNodeDeleteConflictPlans = possibleNodeDeleteConflictPlans.updated(variable, next))
    }

    /**
     * Update [[PossibleDeleteConflictPlans.plansThatReferenceVariable]].
     * This should be called if a plan references a relationship variable.
     */
    def updatePlansThatReferenceRelationshipVariable(plan: LogicalPlan, variable: LogicalVariable): Reads = {
      val prev = possibleRelDeleteConflictPlans.getOrElse(variable, PossibleDeleteConflictPlans(Seq.empty, Seq.empty))
      val next = prev.copy(plansThatReferenceVariable = prev.plansThatReferenceVariable :+ plan)
      copy(possibleRelDeleteConflictPlans = possibleRelDeleteConflictPlans.updated(variable, next))
    }

    /**
     * Return a copy that included the given [[PlanReads]] for the given [[LogicalPlan]]
     */
    def includePlanReads(plan: LogicalPlan, planReads: PlanReads): Reads = {
      Function.chain[Reads](Seq(
        acc => planReads.readNodeProperties.foldLeft(acc)(_.withNodePropertyRead(_, plan)),
        acc => planReads.readLabels.foldLeft(acc)(_.withLabelRead(_, plan)),
        acc => {
          planReads.nodeFilterExpressions.foldLeft(acc) {
            case (acc, (variable, expressions)) =>
              val acc2 = acc.withUpdatedPossibleDeleteNodeConflictPlans(plan, variable, expressions)
              if (expressions.isEmpty) {
                // The plan introduces the variable but has no filter expressions
                acc2.withIntroducedNodeVariable(variable, plan)
              } else {
                expressions.foldLeft(acc2)(_.withAddedNodeFilterExpression(variable, plan, _))
              }
          }
        },
        acc => {
          planReads.referencedNodeVariables.foldLeft(acc) {
            case (acc, variable) => acc.updatePlansThatReferenceNodeVariable(plan, variable)
          }
        },
        acc => {
          planReads.unknownLabelAccessors.foldLeft(acc) {
            case (acc, maybeVar) => acc.withUnknownLabelsRead(plan, maybeVar)
          }
        },
        acc => {
          planReads.unknownNodePropertiesAccessors.foldLeft(acc) {
            case (acc, maybeVar) => acc.withUnknownNodePropertiesRead(plan, maybeVar)
          }
        },
        acc => planReads.readRelProperties.foldLeft(acc)(_.withRelPropertyRead(_, plan)),
        acc => {
          planReads.relationshipFilterExpressions.foldLeft(acc) {
            case (acc, (variable, expressions)) =>
              val acc2 = acc.withUpdatedPossibleRelDeleteConflictPlans(plan, variable, expressions)
              if (expressions.isEmpty) {
                // The plan introduces the variable but has no filter expressions
                acc2.withIntroducedRelationshipVariable(variable, plan)
              } else {
                expressions.foldLeft(acc2)(_.withAddedRelationshipFilterExpression(variable, plan, _))
              }
          }
        },
        acc => {
          planReads.referencedRelationshipVariables.foldLeft(acc) {
            case (acc, variable) => acc.updatePlansThatReferenceRelationshipVariable(plan, variable)
          }
        },
        acc => {
          planReads.unknownRelPropertiesAccessors.foldLeft(acc) {
            case (acc, maybeVar) => acc.withUnknownRelPropertiesRead(plan, maybeVar)
          }
        },
        acc => if (planReads.callInTx) acc.withCallInTx(plan) else acc
      ))(this)
    }

    /**
     * Returns a copy of this class, except that the [[FilterExpressions]] from the other plan are merged in
     * as if it was invoked like `other.mergeWith(this, mergePlan)`.
     */
    def mergeFilterExpressions(
      other: Reads,
      mergePlan: LogicalBinaryPlan
    )(implicit containsOptional: Ref[LogicalPlan] => Boolean): Reads = {
      copy(
        nodeFilterExpressions = other.nodeFilterExpressions.fuse(this.nodeFilterExpressions)(_.mergeWith(_, mergePlan)),
        relationshipFilterExpressions =
          other.relationshipFilterExpressions.fuse(this.relationshipFilterExpressions)(_.mergeWith(_, mergePlan))
      )
    }

    def mergeWith(
      other: Reads,
      mergePlan: LogicalBinaryPlan
    )(implicit containsOptional: Ref[LogicalPlan] => Boolean): Reads = {
      copy(
        readNodeProperties = this.readNodeProperties ++ other.readNodeProperties,
        readLabels = this.readLabels ++ other.readLabels,
        nodeFilterExpressions = this.nodeFilterExpressions.fuse(other.nodeFilterExpressions)(_.mergeWith(_, mergePlan)),
        possibleNodeDeleteConflictPlans =
          this.possibleNodeDeleteConflictPlans.fuse(other.possibleNodeDeleteConflictPlans)(_ ++ _),
        readRelProperties = this.readRelProperties ++ other.readRelProperties,
        relationshipFilterExpressions =
          this.relationshipFilterExpressions.fuse(other.relationshipFilterExpressions)(_.mergeWith(_, mergePlan)),
        possibleRelDeleteConflictPlans =
          this.possibleRelDeleteConflictPlans.fuse(other.possibleRelDeleteConflictPlans)(_ ++ _),
        callInTxPlans = this.callInTxPlans ++ other.callInTxPlans
      )
    }
  }

  /**
   * An accumulator of SETs in the logical plan tree.
   *
   * @param writtenNodeProperties a provider to find out which plans set which properties.
   * @param writtenLabels         for each label, all the plans that set that label.
   */
  private[eager] case class Sets(
    writtenNodeProperties: PropertyWritingPlansProvider = PropertyWritingPlansProvider(),
    writtenRelProperties: PropertyWritingPlansProvider = PropertyWritingPlansProvider(),
    writtenLabels: Map[LabelName, Seq[PlanWithAccessor]] = Map.empty
  ) {

    def withNodePropertyWritten(accessedProperty: AccessedProperty, plan: LogicalPlan): Sets =
      copy(writtenNodeProperties =
        writtenNodeProperties.withNodePropertyWritten(
          accessedProperty.property,
          PlanWithAccessor(plan, accessedProperty.accessor)
        )
      )

    def withRelPropertyWritten(accessedProperty: AccessedProperty, plan: LogicalPlan): Sets =
      copy(writtenRelProperties =
        writtenRelProperties.withRelPropertyWritten(
          accessedProperty.property,
          PlanWithAccessor(plan, accessedProperty.accessor)
        )
      )

    def withUnknownNodePropertyWritten(plan: LogicalPlan, accessor: Option[LogicalVariable]): Sets =
      copy(writtenNodeProperties =
        writtenNodeProperties.withUnknownNodePropertyWritten(PlanWithAccessor(plan, accessor))
      )

    def withUnknownRelPropertyWritten(plan: LogicalPlan, accessor: Option[LogicalVariable]): Sets =
      copy(writtenRelProperties =
        writtenRelProperties.withUnknownRelPropertyWritten(PlanWithAccessor(plan, accessor))
      )

    def withLabelWritten(accessedLabel: AccessedLabel, plan: LogicalPlan): Sets = {
      val wL = writtenLabels.getOrElse(accessedLabel.label, Seq.empty)
      copy(writtenLabels =
        writtenLabels.updated(accessedLabel.label, wL :+ PlanWithAccessor(plan, accessedLabel.accessor))
      )
    }

    def includePlanSets(plan: LogicalPlan, planSets: PlanSets): Sets = {
      Function.chain[Sets](Seq(
        acc => planSets.writtenNodeProperties.foldLeft(acc)(_.withNodePropertyWritten(_, plan)),
        acc => planSets.unknownNodePropertiesAccessors.foldLeft(acc)(_.withUnknownNodePropertyWritten(plan, _)),
        acc => planSets.writtenLabels.foldLeft(acc)(_.withLabelWritten(_, plan)),
        acc => planSets.writtenRelProperties.foldLeft(acc)(_.withRelPropertyWritten(_, plan)),
        acc => planSets.unknownRelPropertiesAccessors.foldLeft(acc)(_.withUnknownRelPropertyWritten(plan, _))
      ))(this)
    }

    def ++(other: Sets): Sets = {
      copy(
        writtenNodeProperties = this.writtenNodeProperties ++ other.writtenNodeProperties,
        writtenLabels = this.writtenLabels.fuse(other.writtenLabels)(_ ++ _),
        writtenRelProperties = this.writtenRelProperties ++ other.writtenRelProperties
      )
    }
  }

  /**
   * An accumulator of CREATEs in the logical plan tree.
   *
   * @param createdNodes                   for each plan, the created nodes.
   * @param nodeFilterExpressionsSnapshots for each plan (that we will need to look at the snapshot later), a snapshot of the current nodeFilterExpressions.
   * @param createdRelationships                   for each plan, the created relationships.
   * @param relationshipFilterExpressionsSnapshots for each plan (that we will need to look at the snapshot later), a snapshot of the current relationshipFilterExpressions.
   *
   */
  private[eager] case class Creates(
    createdNodes: Map[Ref[LogicalPlan], Set[CreatedNode]] = Map.empty,
    nodeFilterExpressionsSnapshots: Map[Ref[LogicalPlan], Map[LogicalVariable, FilterExpressions]] = Map.empty,
    createdRelationships: Map[Ref[LogicalPlan], Set[CreatedRelationship]] = Map.empty,
    relationshipFilterExpressionsSnapshots: Map[Ref[LogicalPlan], Map[LogicalVariable, FilterExpressions]] = Map.empty
  ) {

    /**
     * Save that `plan` creates `createdNode`.
     * Since CREATE plans need to look for conflicts in the _current_ filterExpressions, we save a snapshot of the current filterExpressions,
     * associated with the CREATE plan. If we were to include all filterExpressions, we might think that Eager is not needed, even though it is.
     * See test "inserts eager between Create and NodeByLabelScan if label overlap, and other label in Filter after create".
     */
    def withCreatedNode(
      createdNode: CreatedNode,
      plan: LogicalPlan,
      nodeFilterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions]
    ): Creates = {
      val prevCreatedNodes = createdNodes.getOrElse(Ref(plan), Set.empty)
      copy(
        createdNodes = createdNodes.updated(Ref(plan), prevCreatedNodes + createdNode),
        nodeFilterExpressionsSnapshots = nodeFilterExpressionsSnapshots + (Ref(plan) -> nodeFilterExpressionsSnapshot)
      )
    }

    /**
     * Save that `plan` creates `createdRelationship`.
     * Since CREATE plans need to look for conflicts in the _current_ filterExpressions, we save a snapshot of the current filterExpressions,
     * associated with the CREATE plan. If we were to include all filterExpressions, we might think that Eager is not needed, even though it is.
     * See test "inserts eager between Create and RelationshipTypeScan if Type overlap, and other type in Filter after create".
     */
    def withCreatedRelationship(
      createdRelationship: CreatedRelationship,
      plan: LogicalPlan,
      relationshipFilterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions]
    ): Creates = {
      val prevCreatedRelationships = createdRelationships.getOrElse(Ref(plan), Set.empty)
      copy(
        createdRelationships = createdRelationships.updated(Ref(plan), prevCreatedRelationships + createdRelationship),
        relationshipFilterExpressionsSnapshots =
          relationshipFilterExpressionsSnapshots + (Ref(plan) -> relationshipFilterExpressionsSnapshot)
      )
    }

    def includePlanCreates(
      plan: LogicalPlan,
      planCreates: PlanCreates,
      nodeFilterExpressionsSnapshots: Map[LogicalVariable, FilterExpressions],
      relationshipFilterExpressionsSnapshots: Map[LogicalVariable, FilterExpressions]
    ): Creates = {
      val nodeCreates =
        planCreates.createdNodes.foldLeft(this)(_.withCreatedNode(_, plan, nodeFilterExpressionsSnapshots))
      val relCreates = planCreates.createdRelationships.foldLeft(this)(_.withCreatedRelationship(
        _,
        plan,
        relationshipFilterExpressionsSnapshots
      ))
      nodeCreates ++ relCreates
    }

    def ++(other: Creates): Creates = {
      copy(
        createdNodes = this.createdNodes.fuse(other.createdNodes)(_ ++ _),
        nodeFilterExpressionsSnapshots = this.nodeFilterExpressionsSnapshots ++ other.nodeFilterExpressionsSnapshots,
        createdRelationships = this.createdRelationships.fuse(other.createdRelationships)(_ ++ _),
        relationshipFilterExpressionsSnapshots =
          this.relationshipFilterExpressionsSnapshots ++ other.relationshipFilterExpressionsSnapshots
      )
    }
  }

  /**
   * An accumulator of DELETEs in the logical plan tree.
   *
   * @param deletedNodeVariables                  for each plan, the nodes that are deleted by variable name
   * @param plansThatDeleteNodeExpressions        all plans that delete non-variable expressions of type node
   * @param possibleNodeDeleteConflictPlanSnapshots   for each plan (that we will need to look at the snapshot later), a snapshot of the current possibleNodeDeleteConflictPlans
   * @param deletedRelationshipVariables                  for each plan, the relationships that are deleted by variable name
   * @param plansThatDeleteRelationshipExpressions        all plans that delete non-variable expressions of type relationship
   * @param plansThatDeleteUnknownTypeExpressions all plans that delete expressions of unknown type
   * @param possibleRelationshipDeleteConflictPlanSnapshots   for each plan (that we will need to look at the snapshot later), a snapshot of the current possibleRelationshipDeleteConflictPlans
   */
  private[eager] case class Deletes(
    deletedNodeVariables: Map[Ref[LogicalPlan], Set[Variable]] = Map.empty,
    deletedRelationshipVariables: Map[Ref[LogicalPlan], Set[Variable]] = Map.empty,
    plansThatDeleteNodeExpressions: Seq[LogicalPlan] = Seq.empty,
    plansThatDeleteRelationshipExpressions: Seq[LogicalPlan] = Seq.empty,
    plansThatDeleteUnknownTypeExpressions: Seq[LogicalPlan] = Seq.empty,
    possibleNodeDeleteConflictPlanSnapshots: Map[Ref[LogicalPlan], Map[LogicalVariable, PossibleDeleteConflictPlans]] =
      Map.empty,
    possibleRelationshipDeleteConflictPlanSnapshots: Map[
      Ref[LogicalPlan],
      Map[LogicalVariable, PossibleDeleteConflictPlans]
    ] =
      Map.empty
  ) {

    /**
     * Save that `plan` deletes the variable `deletedNode`.
     */
    def withDeletedNodeVariable(deletedNode: Variable, plan: LogicalPlan): Deletes = {
      val prevDeletedNodes = deletedNodeVariables.getOrElse(Ref(plan), Set.empty)
      copy(deletedNodeVariables = deletedNodeVariables.updated(Ref(plan), prevDeletedNodes + deletedNode))
    }

    /**
     * Save that `plan` deletes the variable `deletedRelationship`.
     */
    def withDeletedRelationshipVariable(deletedRelationship: Variable, plan: LogicalPlan): Deletes = {
      val prevDeletedRelationship = deletedRelationshipVariables.getOrElse(Ref(plan), Set.empty)
      copy(deletedRelationshipVariables =
        deletedRelationshipVariables.updated(Ref(plan), prevDeletedRelationship + deletedRelationship)
      )
    }

    def withPlanThatDeletesNodeExpressions(plan: LogicalPlan): Deletes = {
      copy(plansThatDeleteNodeExpressions = plansThatDeleteNodeExpressions :+ plan)
    }

    def withPlanThatDeletesRelationshipExpressions(plan: LogicalPlan): Deletes = {
      copy(plansThatDeleteRelationshipExpressions = plansThatDeleteRelationshipExpressions :+ plan)
    }

    def withPlanThatDeletesUnknownTypeExpressions(plan: LogicalPlan): Deletes = {
      copy(plansThatDeleteUnknownTypeExpressions = plansThatDeleteUnknownTypeExpressions :+ plan)
    }

    /**
     * Since DELETE plans need to look for the latest plan that references a node variable _before_ the DELETE plan itself,
     * we save a snapshot of the current possibleNodeDeleteConflictPlanSnapshots, associated with the DELETE plan.
     */
    def withPossibleNodeDeleteConflictPlanSnapshot(
      plan: LogicalPlan,
      snapshot: Map[LogicalVariable, PossibleDeleteConflictPlans]
    ): Deletes = {
      copy(possibleNodeDeleteConflictPlanSnapshots =
        possibleNodeDeleteConflictPlanSnapshots.updated(Ref(plan), snapshot)
      )
    }

    /**
     * Since DELETE plans need to look for the latest plan that references a relationship variable _before_ the DELETE plan itself,
     * we save a snapshot of the current possibleRelationshipDeleteConflictPlanSnapshots, associated with the DELETE plan.
     */
    def withPossibleRelationshipDeleteConflictPlanSnapshot(
      plan: LogicalPlan,
      snapshot: Map[LogicalVariable, PossibleDeleteConflictPlans]
    ): Deletes = {
      copy(possibleRelationshipDeleteConflictPlanSnapshots =
        possibleRelationshipDeleteConflictPlanSnapshots.updated(Ref(plan), snapshot)
      )
    }

    def includePlanDeletes(
      plan: LogicalPlan,
      planDeletes: PlanDeletes,
      possibleNodeDeleteConflictPlanSnapshot: Map[LogicalVariable, PossibleDeleteConflictPlans],
      possibleRelationshipDeleteConflictPlanSnapshot: Map[LogicalVariable, PossibleDeleteConflictPlans]
    ): Deletes = {
      Function.chain[Deletes](Seq(
        acc => planDeletes.deletedNodeVariables.foldLeft(acc)(_.withDeletedNodeVariable(_, plan)),
        acc => planDeletes.deletedRelationshipVariables.foldLeft(acc)(_.withDeletedRelationshipVariable(_, plan)),
        acc => if (planDeletes.deletesNodeExpressions) acc.withPlanThatDeletesNodeExpressions(plan) else acc,
        acc =>
          if (planDeletes.deletesRelationshipExpressions) acc.withPlanThatDeletesRelationshipExpressions(plan) else acc,
        acc =>
          if (planDeletes.deletesUnknownTypeExpressions) acc.withPlanThatDeletesUnknownTypeExpressions(plan) else acc,
        acc =>
          if (!planDeletes.isEmpty)
            acc.withPossibleNodeDeleteConflictPlanSnapshot(plan, possibleNodeDeleteConflictPlanSnapshot)
              .withPossibleRelationshipDeleteConflictPlanSnapshot(plan, possibleRelationshipDeleteConflictPlanSnapshot)
          else acc
      ))(this)
    }

    def ++(other: Deletes): Deletes = {
      copy(
        deletedNodeVariables = this.deletedNodeVariables.fuse(other.deletedNodeVariables)(_ ++ _),
        plansThatDeleteNodeExpressions = this.plansThatDeleteNodeExpressions ++ other.plansThatDeleteNodeExpressions,
        plansThatDeleteUnknownTypeExpressions =
          this.plansThatDeleteUnknownTypeExpressions ++ other.plansThatDeleteUnknownTypeExpressions,
        deletedRelationshipVariables =
          this.deletedRelationshipVariables.fuse(other.deletedRelationshipVariables)(_ ++ _),
        plansThatDeleteRelationshipExpressions =
          this.plansThatDeleteRelationshipExpressions ++ other.plansThatDeleteRelationshipExpressions,
        possibleNodeDeleteConflictPlanSnapshots =
          this.possibleNodeDeleteConflictPlanSnapshots ++ other.possibleNodeDeleteConflictPlanSnapshots,
        possibleRelationshipDeleteConflictPlanSnapshots =
          this.possibleRelationshipDeleteConflictPlanSnapshots ++ other.possibleRelationshipDeleteConflictPlanSnapshots
      )
    }
  }

  /**
   * An accumulator of writes  in the logical plan tree.
   */
  private[eager] case class Writes(
    sets: Sets = Sets(),
    creates: Creates = Creates(),
    deletes: Deletes = Deletes()
  ) {

    def withSets(sets: Sets): Writes = copy(sets = sets)

    def withCreates(creates: Creates): Writes = copy(creates = creates)

    def withDeletes(deletes: Deletes): Writes = copy(deletes = deletes)

    def includePlanWrites(
      plan: LogicalPlan,
      planWrites: PlanWrites,
      nodeFilterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions],
      possibleNodeDeleteConflictPlanSnapshot: Map[LogicalVariable, PossibleDeleteConflictPlans],
      relationshipFilterExpressionsSnapshot: Map[LogicalVariable, FilterExpressions],
      possibleRelationshipDeleteConflictPlanSnapshot: Map[LogicalVariable, PossibleDeleteConflictPlans]
    ): Writes = {
      Function.chain[Writes](Seq(
        acc => acc.withSets(acc.sets.includePlanSets(plan, planWrites.sets)),
        acc =>
          acc.withCreates(acc.creates.includePlanCreates(
            plan,
            planWrites.creates,
            nodeFilterExpressionsSnapshot,
            relationshipFilterExpressionsSnapshot
          )),
        acc =>
          acc.withDeletes(acc.deletes.includePlanDeletes(
            plan,
            planWrites.deletes,
            possibleNodeDeleteConflictPlanSnapshot,
            possibleRelationshipDeleteConflictPlanSnapshot
          ))
      ))(this)
    }

    def ++(other: Writes): Writes = {
      copy(
        sets = this.sets ++ other.sets,
        creates = this.creates ++ other.creates,
        deletes = this.deletes ++ other.deletes
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
     * as if it was invoked like `other.mergeWith(this, mergePlan)`.
     */
    def mergeFilterExpressions(
      other: ReadsAndWrites,
      mergePlan: LogicalBinaryPlan
    )(implicit containsOptional: Ref[LogicalPlan] => Boolean): ReadsAndWrites = {
      copy(
        reads = this.reads.mergeFilterExpressions(other.reads, mergePlan)
      )
    }

    def mergeWith(
      other: ReadsAndWrites,
      mergePlan: LogicalBinaryPlan
    )(implicit containsOptional: Ref[LogicalPlan] => Boolean): ReadsAndWrites = {
      copy(
        reads = this.reads.mergeWith(other.reads, mergePlan),
        writes = this.writes ++ other.writes
      )
    }
  }

  /**
   * Traverse plan in execution order and remember all reads and writes.
   */
  private[eager] def collectReadsAndWrites(
    wholePlan: LogicalPlan,
    semanticTable: SemanticTable,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    childrenIds: ChildrenIds
  ): ReadsAndWrites = {
    def processPlan(acc: ReadsAndWrites, plan: LogicalPlan): ReadsAndWrites = {
      val planReads = collectReads(plan, semanticTable, anonymousVariableNameGenerator, childrenIds)
      val planWrites = collectWrites(plan)

      childrenIds.recordChildren(plan)

      Function.chain[ReadsAndWrites](Seq(
        // We update the writes first, because they take snapshots of the reads,
        // and that should happen before the reads of this plan are processed.
        acc => {
          val nodeFilterExpressionsSnapshot = acc.reads.nodeFilterExpressions
          val possibleNodeDeleteConflictPlanSnapshot = acc.reads.possibleNodeDeleteConflictPlans
          val relationshipFilterExpressionsSnapshot = acc.reads.relationshipFilterExpressions
          val possibleRelationshipDeleteConflictPlanSnapshot = acc.reads.possibleRelDeleteConflictPlans
          acc.withWrites(acc.writes.includePlanWrites(
            plan,
            planWrites,
            nodeFilterExpressionsSnapshot,
            possibleNodeDeleteConflictPlanSnapshot,
            relationshipFilterExpressionsSnapshot,
            possibleRelationshipDeleteConflictPlanSnapshot
          ))
        },
        acc => acc.withReads(acc.reads.includePlanReads(plan, planReads))
      ))(acc)
    }

    implicit val containsOptionalCached: Ref[LogicalPlan] => Boolean = CachedFunction {
      planRef =>
        planRef.value.folder.treeExists {
          case _: Optional => true
        }
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
            val acc = lhsAcc.mergeWith(rhsAcc, plan)
            processPlan(acc, plan)
        }
    )
  }
}
