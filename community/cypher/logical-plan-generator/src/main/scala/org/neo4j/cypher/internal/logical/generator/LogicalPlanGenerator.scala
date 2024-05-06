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
package org.neo4j.cypher.internal.logical.generator

import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.generator.AstGenerator.zeroOrMore
import org.neo4j.cypher.internal.ast.generator.SemanticAwareAstGenerator
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeZipper
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.compiler.helpers.PredicateHelper
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.shouldPlanExhaustiveLimit
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext.Results
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.State
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.attribution.Default
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.kernel.api.StatementConstants
import org.scalacheck.Gen

import scala.language.implicitConversions

object LogicalPlanGenerator extends AstConstructionTestSupport {
  case class WithState[+T](x: T, state: State)

  object State {

    def apply(labelsWithIds: Map[String, Int], relTypesWithIds: Map[String, Int]): State = {
      val resolvedLabelTypes = Map(labelsWithIds.view.mapValues(LabelId).toSeq: _*)
      val resolvedRelTypes = Map(relTypesWithIds.view.mapValues(RelTypeId).toSeq: _*)
      State(
        new SemanticTable(
          resolvedLabelNames = resolvedLabelTypes,
          resolvedRelTypeNames = resolvedRelTypes
        ),
        Set.empty,
        0,
        Set.empty,
        List(Cardinality.SINGLE),
        Map.empty.withDefaultValue(Set.empty),
        Map.empty,
        new Cardinalities,
        new SequentialIdGen()
      )
    }
  }

  /**
   * Accumulated state while generating logical plans
   * @param semanticTable the semantic table
   * @param arguments arguments, which are valid at this point of generation.
   * @param varCount the amount of generated distinct variables
   * @param parameters the generated parameter names
   * @param leafCardinalityMultipliersStack a stack of cardinalities from LHS of enclosing `Apply`s
   * @param labelInfo generated node variables with labels
   * @param cardinalities cardinalities of generated plans
   * @param idGen id generator for plans
   */
  case class State(
    semanticTable: SemanticTable,
    arguments: Set[LogicalVariable],
    varCount: Int,
    parameters: Set[String],
    private val leafCardinalityMultipliersStack: List[Cardinality],
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    cardinalities: Cardinalities,
    idGen: IdGen
  ) {

    def incVarCount(): State =
      copy(varCount = varCount + 1)

    def newNode(name: Variable): State =
      copy(semanticTable = semanticTable.addNode(name))

    def newRelationship(name: Variable): State =
      copy(semanticTable = semanticTable.addRelationship(name))

    def declareTypeAny(name: Variable): State =
      copy(semanticTable =
        semanticTable.copy(types = semanticTable.types.updated(name, ExpressionTypeInfo(CTAny.invariant, None)))
      )

    def addArguments(args: Set[LogicalVariable]): State =
      copy(arguments = arguments ++ args)

    def removeArguments(args: Set[LogicalVariable]): State =
      copy(arguments = arguments -- args)

    def addParameters(ps: Set[String]): State =
      copy(parameters = parameters ++ ps)

    def pushLeafCardinalityMultiplier(c: Cardinality): State =
      copy(leafCardinalityMultipliersStack = c +: leafCardinalityMultipliersStack)

    def popLeafCardinalityMultiplier(): State =
      copy(leafCardinalityMultipliersStack = leafCardinalityMultipliersStack.tail)

    def leafCardinalityMultiplier: Cardinality =
      leafCardinalityMultipliersStack.headOption.getOrElse(Cardinality.SINGLE)

    def recordLabel(variable: Variable, label: String): State = {
      val newLabels = labelInfo(variable) + LabelName(label)(pos)
      copy(labelInfo = labelInfo.updated(variable, newLabels))
    }
  }
}

/**
 * A generator of random logical plans, with the ambition to generate only valid plans.
 * @param labelsWithIds The labels that exist in a graph that the plans can be executed against.
 * @param relTypesWithIds The relationship types that exist in a graph that the plans can be executed against.
 * @param planContext Mostly used to obtain statistics of a graphs that plans are executed against.
 * @param costLimit Maximum allowed cost of a generated plan.
 */
class LogicalPlanGenerator(
  labelsWithIds: Map[String, Int],
  relTypesWithIds: Map[String, Int],
  planContext: PlanContext,
  costLimit: Cost,
  nodes: Seq[Node],
  rels: Seq[Relationship]
) extends AstConstructionTestSupport {

  private val labels = labelsWithIds.keys.toVector
  private val relTypes = relTypesWithIds.keys.toVector
  private val relIds = rels.map(_.getId)

  /**
   * A convenience conversion that allows us to mix LogicalPlan Gens with State mutators in the same for comprehension.
   * A simplified example looks like this:
   * {{{
   *   for {
   *       WithState(source, state) <- innerLogicalPlan(state)
   *       from <- oneOf(source.availableSymbols.toSeq)
   *       dir <- semanticDirection
   *       relTypes <- relTypeNames
   *       WithState(to, state) <- newVariable(state)
   *       state <- state.newNode(to)
   *       WithState(rel, state) <- newVariable(state)
   *       state <- state.newRelationship(rel)
   *     } yield {
   *       WithState(Expand(source, from, dir, relTypes, to, rel)(state.idGen), state)
   *     }
   * }}}
   * Like this we shadow `state` with a new variable called `state` each time we obtain a new state.
   * This makes it quite easy to modify these comprehensions without having to change a lot of references that would now have to point to the new state
   * and are easy to miss.
   */
  implicit private def stateToGen(s: State): Gen[State] = Gen.const(s)

  /**
   * Main entry point to obtain logical plans and associated state.
   */
  def logicalPlan: Gen[WithState[LogicalPlan]] = for {
    initialState <- Gen.delay(State(labelsWithIds, relTypesWithIds))
    WithState(source, state) <- innerLogicalPlan(initialState)
  } yield annotate(ProduceResult.withNoCachedProperties(source, source.availableSymbols.toSeq)(state.idGen), state)

  def innerLogicalPlan(state: State): Gen[WithState[LogicalPlan]] = Gen.oneOf(
    leafPlan(state),
    oneChildPlan(state),
    twoChildPlan(state)
  ).suchThat {
    case WithState(plan, state) =>
      val po = new ProvidedOrders with Default[LogicalPlan, ProvidedOrder] {
        override protected def defaultValue: ProvidedOrder = ProvidedOrder.empty
      }
      CardinalityCostModel(Volcano, CancellationChecker.neverCancelled())
        .costFor(
          plan,
          QueryGraphSolverInput.empty,
          state.semanticTable,
          state.cardinalities,
          po,
          Set.empty,
          planContext.statistics,
          CostModelMonitor.DEFAULT
        ) <= costLimit
  }

  def innerLogicalPlanWithAtLeastOneSymbol(state: State): Gen[WithState[LogicalPlan]] =
    innerLogicalPlan(state).suchThat {
      case WithState(plan, _) => plan.availableSymbols.nonEmpty
    }

  def leafPlan(state: State): Gen[WithState[LogicalPlan]] = Gen.oneOf(
    argument(state),
    allNodesScan(state),
    nodeByLabelScan(state),
    sortedUndirectedRelationshipByIdSeek(state),
    sortedDirectedRelationshipByIdSeek(state),
    nodeCountFromCountStore(state),
    relCountFromCountStore(state)
  )

  def oneChildPlan(state: State): Gen[WithState[LogicalPlan]] = Gen.oneOf(
    Gen.lzy(eager(state)),
    Gen.lzy(expand(state)),
    Gen.lzy(skip(state)),
    Gen.lzy(limit(state)),
    Gen.lzy(projection(state)),
    Gen.lzy(aggregation(state)),
    Gen.lzy(distinct(state)),
    Gen.lzy(optional(state)),
    Gen.lzy(sort(state)),
    Gen.lzy(top(state)),
    Gen.lzy(selection(state)),
    Gen.lzy(unwindCollection(state))
  )

  def twoChildPlan(state: State): Gen[WithState[LogicalPlan]] = Gen.oneOf(
    Gen.lzy(cartesianProduct(state)),
    Gen.lzy(union(state)),
    Gen.lzy(apply(state)),
    Gen.lzy(semiApply(state)),
    Gen.lzy(antiSemiApply(state)),
    Gen.lzy(valueHashJoin(state))
  )

  // Leaf Plans

  def allNodesScan(state: State): Gen[WithState[AllNodesScan]] = for {
    WithState(node, state) <- newVariable(state)
    state <- state.newNode(node)
  } yield {
    val plan = AllNodesScan(node, state.arguments)(state.idGen)
    annotate(plan, state)
  }

  def nodeByLabelScan(state: State): Gen[WithState[NodeByLabelScan]] = for {
    labelName <- label
    io <- indexOrder
    WithState(node, state) <- newVariable(state)
    state <- state.newNode(node)
    state <- state.recordLabel(node, labelName.name)
  } yield {
    val plan = NodeByLabelScan(node, labelName, state.arguments, io)(state.idGen)
    annotate(plan, state)
  }

  def argument(state: State): Gen[WithState[Argument]] = {
    val plan = Argument(state.arguments)(state.idGen)
    Gen.const(annotate(plan, state))
  }

  private def nodeCountFromCountStore(state: State): Gen[WithState[NodeCountFromCountStore]] = for {
    WithState(idName, state) <- newVariable(state)
    state <- state.declareTypeAny(idName)
    labels <- Gen.listOf(optionalLabel)
  } yield {
    val plan = NodeCountFromCountStore(idName, labels, state.arguments)(state.idGen)
    annotate(plan, state)
  }

  private def relCountFromCountStore(state: State): Gen[WithState[RelationshipCountFromCountStore]] = for {
    WithState(idName, state) <- newVariable(state)
    state <- state.declareTypeAny(idName)
    label <- optionalLabel
    (startLabel, endLabel) <- Gen.oneOf(label -> None, None -> label)
    typeNames <- relTypeNames
  } yield {
    val plan = RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, state.arguments)(state.idGen)
    annotate(plan, state)
  }

  // One child plans

  def eager(state: State): Gen[WithState[Eager]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
  } yield {
    val plan = Eager(source)(state.idGen)
    annotate(plan, state)
  }

  def expand(state: State): Gen[WithState[Expand]] = for {
    WithState(source, state) <- innerLogicalPlan(state).suchThat { case WithState(plan, state) =>
      plan.availableSymbols.exists(v => state.semanticTable.typeFor(v).is(CTNode))
    }
    from <-
      Gen.oneOf(source.availableSymbols.toSeq).suchThat(name => state.semanticTable.typeFor(name).is(CTNode))
    dir <- semanticDirection
    relTypes <- relTypeNames
    WithState(to, state) <- newVariable(state)
    state <- state.newNode(to)
    WithState(rel, state) <- newVariable(state)
    state <- state.newRelationship(rel)
  } yield {
    val plan = Expand(source, from, dir, relTypes, to, rel)(state.idGen)
    annotate(plan, state)
  }

  private def sortedRelationshipByIdSeek(state: State, directed: Boolean): Gen[WithState[Sort]] = for {
    WithState(idName, state) <- newVariable(state)
    state <- state.newRelationship(idName)
    WithState(left, state) <- newVariable(state)
    state <- state.newNode(left)
    WithState(right, state) <- newVariable(state)
    state <- state.newNode(right)
    relIds <- Gen.someOf(relIds ++ Seq.fill(relIds.size)(StatementConstants.NO_SUCH_RELATIONSHIP))
  } yield {
    val seekableArgs = ManySeekableArgs(listOfInt(relIds.toSeq: _*))
    val plan =
      if (directed) {
        val p = DirectedRelationshipByIdSeek(idName, seekableArgs, left, right, Set.empty)(state.idGen)
        annotate(p, state)
        p
      } else {
        val p = UndirectedRelationshipByIdSeek(idName, seekableArgs, left, right, Set.empty)(state.idGen)
        annotate(p, state)
        p
      }

    // result order is undefined, sort to make sure we get the same result for all runtimes
    val sortPlan = Sort(plan, Seq(Ascending(left), Ascending(idName)))(state.idGen)
    annotate(sortPlan, state)
  }

  def sortedUndirectedRelationshipByIdSeek(state: State): Gen[WithState[Sort]] =
    sortedRelationshipByIdSeek(state, directed = false)

  def sortedDirectedRelationshipByIdSeek(state: State): Gen[WithState[Sort]] =
    sortedRelationshipByIdSeek(state, directed = true)

  def skip(state: State): Gen[WithState[Skip]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    count <- Gen.chooseNum(0, Long.MaxValue, 1)
  } yield {
    val plan = Skip(source, literalInt(count))(state.idGen)
    annotate(plan, state)
  }

  def limit(state: State): Gen[WithState[LogicalPlan]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    count <- Gen.chooseNum(0, Long.MaxValue, 1)
  } yield {
    if (shouldPlanExhaustiveLimit(source, Some(count))) {
      annotate(ExhaustiveLimit(source, literalInt(count))(state.idGen), state)
    } else {
      annotate(Limit(source, literalInt(count))(state.idGen), state)
    }
  }

  def projection(state: State): Gen[WithState[Projection]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    WithState(map, state) <- projectionList(state, source.availableSymbols.toSeq, _.nonAggregatingExpression)
  } yield {
    val plan = Projection(source, map)(state.idGen)
    annotate(plan, state)
  }

  private def projectionList(
    state: State,
    availableSymbols: Seq[LogicalVariable],
    expressionGen: SemanticAwareAstGenerator => Gen[Expression],
    minSize: Int = 0
  ): Gen[WithState[Map[LogicalVariable, Expression]]] =
    Gen.sized(s => Gen.choose(minSize, s max minSize)).flatMap { n =>
      (0 until n).foldLeft(Gen.const(WithState(Map.empty[LogicalVariable, Expression], state))) { (prevGen, _) =>
        for {
          WithState(map, state) <- prevGen
          WithState(name, state) <- newVariable(state)
          WithState(xpr, state) <- validExpression(availableSymbols, state, expressionGen)
          state <- state.declareTypeAny(name)
        } yield {
          WithState(map + (name -> xpr), state)
        }
      }
    }

  def aggregation(state: State): Gen[WithState[Aggregation]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    WithState(groupingExpressions, state) <-
      projectionList(state, source.availableSymbols.toSeq, _.nonAggregatingExpression)
    WithState(aggregatingExpressions, state) <-
      projectionList(state, source.availableSymbols.toSeq, _.aggregatingExpression, minSize = 1)
  } yield {
    val plan = Aggregation(source, groupingExpressions, aggregatingExpressions)(state.idGen)
    annotate(plan, state)
  }

  def distinct(state: State): Gen[WithState[Distinct]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    WithState(groupingExpressions, state) <-
      projectionList(state, source.availableSymbols.toSeq, _.nonAggregatingExpression, minSize = 1)
  } yield {
    val plan = Distinct(source, groupingExpressions)(state.idGen)
    annotate(plan, state)
  }

  def optional(state: State): Gen[WithState[Optional]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
  } yield {
    val plan = Optional(source, state.arguments)(state.idGen)
    annotate(plan, state)
  }

  def sort(state: State): Gen[WithState[Sort]] = for {
    WithState(source, state) <- innerLogicalPlanWithAtLeastOneSymbol(state)
    columns <- Gen.atLeastOne(source.availableSymbols)
    orderings <- Gen.listOfN(columns.size, Gen.oneOf(Ascending, Descending))
  } yield {
    val orderedColumns = columns.zip(orderings).map { case (column, order) => order(column) }
    val plan = Sort(source, orderedColumns.toSeq)(state.idGen)
    annotate(plan, state)
  }

  def top(state: State): Gen[WithState[Top]] = for {
    WithState(source, state) <- innerLogicalPlanWithAtLeastOneSymbol(state)
    columns <- Gen.atLeastOne(source.availableSymbols)
    orderings <- Gen.listOfN(columns.size, Gen.oneOf(Ascending, Descending))
    count <- Gen.chooseNum(0, Long.MaxValue, 1)
  } yield {
    val orderedColumns = columns.zip(orderings).map { case (column, order) => order(column) }
    val plan = Top(source, orderedColumns.toSeq, literalInt(count))(state.idGen)
    annotate(plan, state)
  }

  def selection(state: State): Gen[WithState[Selection]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    WithState(xpr, state) <- expressionList(state, source.availableSymbols.toSeq, _.nonAggregatingExpression, 1)
  } yield {
    val plan = Selection(PredicateHelper.coercePredicatesWithAnds(xpr).get, source)(state.idGen)
    annotate(plan, state)
  }

  def unwindCollection(state: State): Gen[WithState[UnwindCollection]] = for {
    WithState(source, state) <- innerLogicalPlanWithAtLeastOneSymbol(state)
    WithState(xpr, state) <- validExpression(
      source.availableSymbols.toSeq,
      state,
      expressionGen => expressionGen._listOf(expressionGen.nonAggregatingExpression)
    )
    WithState(name, state) <- newVariable(state)
    state <- state.declareTypeAny(name)
  } yield {
    val plan = UnwindCollection(source, name, xpr)(state.idGen)
    annotate(plan, state)
  }

  // Two child plans

  def antiSemiApply(state: State): Gen[WithState[AntiSemiApply]] = for {
    WithState((left, right), state) <- getApplyInnerPlans(state)
  } yield {
    val plan = AntiSemiApply(left, right)(state.idGen)
    annotate(plan, state)
  }

  def semiApply(state: State): Gen[WithState[SemiApply]] = for {
    WithState((left, right), state) <- getApplyInnerPlans(state)
  } yield {
    val plan = SemiApply(left, right)(state.idGen)
    annotate(plan, state)
  }

  def apply(state: State): Gen[WithState[Apply]] = for {
    WithState((left, right), state) <- getApplyInnerPlans(state)
  } yield {
    val plan = Apply(left, right)(state.idGen)
    annotate(plan, state)
  }

  private def getApplyInnerPlans(state: State): Gen[WithState[(LogicalPlan, LogicalPlan)]] = for {
    WithState(left, state) <- innerLogicalPlan(state)
    newArguments = left.availableSymbols -- state.arguments
    state <- state.addArguments(newArguments)
    state <- state.pushLeafCardinalityMultiplier(state.cardinalities.get(left.id))
    WithState(right, state) <- innerLogicalPlan(state)
    state <- state.removeArguments(newArguments)
    state <- state.popLeafCardinalityMultiplier()
  } yield WithState((left, right), state)

  private def cartesianProduct(state: State): Gen[WithState[CartesianProduct]] = for {
    WithState(left, state) <- innerLogicalPlan(state)
    WithState(right, state) <- innerLogicalPlan(state)
  } yield {
    val plan = CartesianProduct(left, right)(state.idGen)
    annotate(plan, state)
  }

  def union(state: State): Gen[WithState[Union]] = for {
    WithState(left, state) <- innerLogicalPlan(state)
    WithState(right, state) <- genPlanWithSameAvailableSymbols(left, state)
  } yield {
    // use a copy of left plan as right hand side in order to have the same available symbols
    val plan = Union(left, right)(state.idGen)
    annotate(plan, state)
  }

  private def valueHashJoin(state: State): Gen[WithState[ValueHashJoin]] = for {
    WithState(left, state) <- innerLogicalPlanWithAtLeastOneSymbol(state)
    WithState(right, state) <- innerLogicalPlanWithAtLeastOneSymbol(state)
    WithState(leftExpr, state) <- valueHashJoinExpression(left, state)
    WithState(rightExpr, state) <- valueHashJoinExpression(right, state)
  } yield {
    val equalsExpr = equals(leftExpr, rightExpr)
    val plan = ValueHashJoin(left, right, equalsExpr)(state.idGen)
    annotate(plan, state)
  }

  // Other stuff

  private def annotate[T <: LogicalPlan](plan: T, state: State)(implicit
  cardinalityCalculator: CardinalityCalculator[T]): WithState[T] = {
    state.cardinalities.set(plan.id, cardinalityCalculator(plan, state, planContext, labelsWithIds))
    WithState(plan, state)
  }

  private def semanticDirection: Gen[SemanticDirection] = Gen.oneOf(
    SemanticDirection.INCOMING,
    SemanticDirection.OUTGOING,
    SemanticDirection.BOTH
  )

  private def indexOrder: Gen[IndexOrder] = Gen.oneOf(
    IndexOrderNone,
    IndexOrderAscending,
    IndexOrderAscending
  )

  private def label: Gen[LabelName] = for {
    name <- Gen.oneOf(labels)
  } yield LabelName(name)(pos)

  private def optionalLabel: Gen[Option[LabelName]] = Gen.option(label)

  private def relTypeNames: Gen[Seq[RelTypeName]] = for {
    names <- zeroOrMore(relTypes)
    name <- names
  } yield RelTypeName(name)(pos)

  def newVariable(state: State): Gen[WithState[Variable]] = {
    val name = s"var${state.varCount}"
    Gen.const(WithState(varFor(name), state.incVarCount()))
  }

  private def expressionList(
    state: State,
    availableSymbols: Seq[LogicalVariable],
    expressionGen: SemanticAwareAstGenerator => Gen[Expression],
    minSize: Int
  ): Gen[WithState[Seq[Expression]]] =
    Gen.sized(s => Gen.choose(minSize, s max minSize)).flatMap { n =>
      (0 until n).foldLeft(Gen.const(WithState(Seq.empty[Expression], state))) { (prevGen, _) =>
        for {
          WithState(xprs, state) <- prevGen
          WithState(xpr, state) <- validExpression(availableSymbols, state, expressionGen)
        } yield {
          WithState(xprs :+ xpr, state)
        }
      }
    }

  private def genPlanWithSameAvailableSymbols(plan: LogicalPlan, state: State): Gen[WithState[LogicalPlan]] = for {
    // We need to create a new state in order to be able to get the same variables as `plan`
    rhsState <- Gen.delay(copyStateWithoutVariableInfo(state))
    WithState(newPlan, newState) <- innerLogicalPlan(rhsState)
      .suchThat {
        case WithState(source, _) => source.availableSymbols == plan.availableSymbols
      }
  } yield {
    WithState(newPlan, state.addParameters(newState.parameters))
  }

  /*
   * Creates copy of state, without variable information.
   *
   * - Shares cardinalities with state
   * - Shares idGen with state
   */
  private def copyStateWithoutVariableInfo(state: State) = {
    val resolvedLabelTypes = Map(labelsWithIds.mapValues(LabelId).toSeq: _*)
    val resolvedRelTypes = Map(relTypesWithIds.mapValues(RelTypeId).toSeq: _*)
    val arguments = state.arguments
    val variables = arguments.map(_.asInstanceOf[Expression])

    val semanticTable = new SemanticTable(
      resolvedLabelNames = resolvedLabelTypes,
      resolvedRelTypeNames = resolvedRelTypes,
      types = ASTAnnotationMap(state.semanticTable.types.filterKeys(pn => variables.contains(pn.node)).toList: _*)
    )

    State(
      semanticTable,
      arguments,
      arguments.size,
      state.parameters,
      List(state.leafCardinalityMultiplier),
      Map.empty.withDefaultValue(Set.empty),
      Map.empty,
      state.cardinalities,
      state.idGen
    )
  }

  /**
   * This generates random expressions and then uses SematicChecking to see if they are valid. This works,
   * but is inefficient and will miss lots of expressions for which it is harder to generate valid instances.
   */
  private def validExpression(
    availableSymbols: Seq[LogicalVariable],
    state: State,
    expressionGen: SemanticAwareAstGenerator => Gen[Expression]
  ): Gen[WithState[Expression]] = {
    val semanticState = SemanticState(
      new ScopeLocation(Scope.empty.location(ScopeZipper)),
      state.semanticTable.types,
      ASTAnnotationMap.empty
    )
    for {
      expression <- expressionGen(new SemanticAwareAstGenerator(allowedVarNames = Some(availableSymbols.map(_.name))))
        .suchThat(e => {
          val errors = SemanticExpressionCheck.check(Results, e).run(
            semanticState,
            new SemanticCheckContext {
              override def errorMessageProvider: ErrorMessageProvider = MessageUtilProvider
            }
          ).errors
          errors.isEmpty
        })
      parameters = expression.folder.findAllByClass[Parameter].map(_.name)
      state <- state.addParameters(parameters.toSet)
    } yield {
      WithState(expression, state)
    }
  }

  private def valueHashJoinExpression(plan: LogicalPlan, state: State): Gen[WithState[Expression]] = {
    val symbols = plan.availableSymbols.toSeq
    Gen.oneOf(
      validExpression(symbols, state, _.nonAggregatingExpression),
      Gen.oneOf(symbols).map(s => WithState(s, state))
    )
  }
}
