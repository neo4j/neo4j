/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.logical.generator

import org.neo4j.cypher.internal.ast.ASTAnnotationMap
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.generator.AstGenerator.zeroOrMore
import org.neo4j.cypher.internal.ast.generator.SemanticAwareAstGenerator
import org.neo4j.cypher.internal.ast.semantics.ExpressionTypeInfo
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeZipper
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext.Results
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.State
import org.neo4j.cypher.internal.logical.generator.LogicalPlanGenerator.WithState
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.IncludeTies
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.scalacheck.Gen
import org.scalacheck.Gen.choose
import org.scalacheck.Gen.chooseNum
import org.scalacheck.Gen.const
import org.scalacheck.Gen.delay
import org.scalacheck.Gen.lzy
import org.scalacheck.Gen.oneOf
import org.scalacheck.Gen.sized

object LogicalPlanGenerator extends AstConstructionTestSupport {
  case class WithState[+T](x: T, state: State)

  object State {
    def apply(): State =
      State(
        SemanticTable(),
        Set.empty,
        0,
        Set.empty,
        List(Cardinality.SINGLE),
        Map.empty.withDefaultValue(Set.empty),
        new Cardinalities,
        new SequentialIdGen())
  }

  /**
   * Accumulated state while generating logical plans
   * @param semanticTable the semantic table
   * @param arguments arguments, which are valid at this point of generation.
   * @param varCount the amount of generated distinct variables
   * @param parameters the generated parameter names
   */
  case class State(semanticTable: SemanticTable,
                   arguments: Set[String],
                   varCount: Int,
                   parameters: Set[String],
                   leafCardinalityMultipliers: List[Cardinality],
                   labelInfo: LabelInfo,
                   cardinalities: Cardinalities,
                   idGen: SequentialIdGen) {

    /**
     * These mutator methods on state do not simply return a new State, but rather a Gen of a State.
     * The reason is that like that we can mix LogicalPlan Gens with State mutators in the same for comprehension.
     *
     * A simplified example looks like this:
     * {{{
     *   for {
     *       WithState(source, state) <- innerLogicalPlan(state)
     *       from <- oneOf(source.availableSymbols.toSeq)
     *       dir <- semanticDirection
     *       relTypes <- relTypeNames
     *       WithState(to, state) <- state.newVariable
     *       state <- state.newNode(to)
     *       WithState(rel, state) <- state.newVariable
     *       state <- state.newRelationship(rel)
     *     } yield {
     *       WithState(Expand(source, from, dir, relTypes, to, rel)(state.idGen), state)
     *     }
     * }}}
     *
     * Like this we shadow `state` with a new variable called `state` each time we obtain a new state.
     * This makes it quite easy to modify these comprehensions without having to change a lot of references that would now have to point to the new state
     * and are easy to miss.
     */

    def newVariable: Gen[WithState[String]] = {
      val name = s"var$varCount"
      const(WithState(name, copy(varCount = varCount + 1)))
    }

    def newNode(name: String): Gen[State] =
      const(copy(semanticTable = semanticTable.addNode(varFor(name))))

    def newRelationship(name: String): Gen[State] =
      const(copy(semanticTable = semanticTable.addRelationship(varFor(name))))

    def declareTypeAny(name: String): Gen[State] =
      const(copy(semanticTable = semanticTable.copy(types = semanticTable.types.updated(varFor(name), ExpressionTypeInfo(CTAny.invariant, None)))))

    def addArguments(args: Set[String]): Gen[State] =
      const(copy(arguments = arguments ++ args))

    def removeArguments(args: Set[String]): Gen[State] =
      const(copy(arguments = arguments -- args))

    def addParameters(ps: Set[String]): Gen[State] =
      const(copy(parameters = parameters ++ ps))

    def pushLeafCardinalityMultiplier(c: Cardinality): State =
      copy(leafCardinalityMultipliers = c +: leafCardinalityMultipliers)

    def popLeafCardinalityMultiplier(): Gen[State] =
      const(copy(leafCardinalityMultipliers = leafCardinalityMultipliers.tail))

    def recordLabel(variable: String, label: String): Gen[State] = {
      val newLabels = labelInfo(label) + LabelName(label)(pos)
      const(copy(labelInfo = labelInfo.updated(variable, newLabels)))
    }
  }
}

/**
 * A generator of random logical plans, with the ambition to generate only valid plans.
 * @param labelsWithIds The labels that exist in a graph that the plans can be executed against.
 * @param relTypes The relationship types that exist in a graph that the plans can be executed against.
 * @param stats Statistics of a graphs that plans are executed against.
 * @param costLimit Maximum allowed cost of a generated plan.
 */
class LogicalPlanGenerator(labelsWithIds: Map[String, Int], relTypes: Seq[String], stats: GraphStatistics, costLimit: Cost) extends AstConstructionTestSupport {

  private val labels = labelsWithIds.keys.toVector
  private val costModel = CardinalityCostModel(null)

  /**
   * Main entry point to obtain logical plans and associated state.
   */
  def logicalPlan: Gen[WithState[LogicalPlan]] = for {
    initialState <- delay(State())
    WithState(source, state) <- innerLogicalPlan(initialState)
  } yield annotate(ProduceResult(source, source.availableSymbols.toSeq)(state.idGen), state)

  def innerLogicalPlan(state: State): Gen[WithState[LogicalPlan]] = oneOf(
    argument(state),
    allNodesScan(state),
    nodeByLabelScan(state),
    lzy(expand(state)),
    lzy(skip(state)),
    lzy(limit(state)),
    lzy(projection(state)),
    lzy(aggregation(state)),
    lzy(cartesianProduct(state)),
    lzy(apply(state))
  ).suchThat {
    case WithState(plan, state) => costModel.apply(plan, QueryGraphSolverInput.empty, state.cardinalities) <= costLimit
  }

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
    WithState(node, state) <- newVariable(state)
    state <- state.newNode(node)
    state <- state.recordLabel(node, labelName.name)
  } yield {
    val plan = NodeByLabelScan(node, labelName, state.arguments)(state.idGen)
    annotate(plan, state)
  }

  def argument(state: State): Gen[WithState[Argument]] = {
    val plan = Argument(state.arguments)(state.idGen)
    const(annotate(plan, state))
  }

  // One child plans

  def expand(state: State): Gen[WithState[Expand]] = for {
    WithState(source, state) <- innerLogicalPlan(state).suchThat { case WithState(plan, state) => plan.availableSymbols.exists(v => state.semanticTable.isNode(v)) }
    from <- oneOf(source.availableSymbols.toSeq).suchThat(name => state.semanticTable.isNode(varFor(name)))
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

  def skip(state: State): Gen[WithState[Skip]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    count <- chooseNum(0, Long.MaxValue, 1)
  } yield {
    val plan = Skip(source, literalInt(count))(state.idGen)
    annotate(plan, state)
  }

  def limit(state: State): Gen[WithState[Limit]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    count <- chooseNum(0, Long.MaxValue, 1)
    ties <- if (source.isInstanceOf[Sort] && count == 1) oneOf(DoNotIncludeTies, IncludeTies) else const(DoNotIncludeTies)
  } yield {
    val plan = Limit(source, literalInt(count), ties)(state.idGen)
    annotate(plan, state)
  }

  def projection(state: State): Gen[WithState[Projection]] = for {
    WithState(source, state) <- innerLogicalPlan(state)
    WithState(map, state) <- projectionList(state, source.availableSymbols.toSeq, _.nonAggregatingExpression)
  } yield {
    val plan = Projection(source, map)(state.idGen)
    annotate(plan, state)
  }

  private def projectionList(state: State, availableSymbols: Seq[String], expressionGen: SemanticAwareAstGenerator => Gen[Expression], minSize: Int = 0): Gen[WithState[Map[String, Expression]]] =
    sized(s => choose(minSize, s max minSize)).flatMap { n =>
      (0 until n).foldLeft(const(WithState(Map.empty[String, Expression], state))) { (prevGen, _) =>
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
    WithState(groupingExpressions, state) <- projectionList(state, source.availableSymbols.toSeq, _.nonAggregatingExpression)
    WithState(aggregatingExpressions, state) <- projectionList(state, source.availableSymbols.toSeq, _.aggregatingExpression, minSize = 1)
  } yield {
    val plan = Aggregation(source, groupingExpressions, aggregatingExpressions)(state.idGen)
    annotate(plan, state)
  }

  // Two child plans

  def apply(state: State): Gen[WithState[Apply]] = for {
    WithState(left, state) <- innerLogicalPlan(state)
    state <- state.addArguments(left.availableSymbols)
    state <- const(state.pushLeafCardinalityMultiplier(state.cardinalities.get(left.id)))
    WithState(right, state) <- innerLogicalPlan(state)
    state <- state.removeArguments(left.availableSymbols)
    state <- state.popLeafCardinalityMultiplier()
  } yield {
    val plan = Apply(left, right)(state.idGen)
    annotate(plan, state)
  }

  def cartesianProduct(state: State): Gen[WithState[CartesianProduct]] = for {
    WithState(left, state) <- innerLogicalPlan(state)
    WithState(right, state) <- innerLogicalPlan(state)
  } yield {
    val plan = CartesianProduct(left, right)(state.idGen)
    annotate(plan, state)
  }

  // Other stuff

  private def annotate[T <: LogicalPlan](plan: T, state: State)(implicit cardinalityCalculator: CardinalityCalculator[T]): WithState[T] = {
    state.cardinalities.set(plan.id, cardinalityCalculator(plan, state, stats, labelsWithIds))
    WithState(plan, state)
  }

  private def semanticDirection: Gen[SemanticDirection] = oneOf(
    SemanticDirection.INCOMING,
    SemanticDirection.OUTGOING,
    SemanticDirection.BOTH
  )

  private def label: Gen[LabelName] = for {
    name <- oneOf(labels)
  } yield LabelName(name)(pos)

  private def relTypeNames: Gen[Seq[RelTypeName]] = for {
    names <- zeroOrMore(relTypes)
    name <- names
  } yield RelTypeName(name)(pos)

  // State

  def newVariable(state: State): Gen[WithState[String]] =
    state.newVariable

//  {
//    val name = s"var${state.varCount}"
//    const(WithState(name, state.newVariable()))
//  }

//  def newNode(name: String): Gen[State] =
//    const(copy(semanticTable = semanticTable.addNode(varFor(name))))
//
//  def newRelationship(name: String): Gen[State] =
//    const(copy(semanticTable = semanticTable.addRelationship(varFor(name))))
//
//  def declareTypeAny(name: String): Gen[State] =
//    const(copy(semanticTable = semanticTable.copy(types = semanticTable.types.updated(varFor(name), ExpressionTypeInfo(CTAny.invariant, None)))))
//
//  def addArguments(args: Set[String]): Gen[State] =
//    const(copy(arguments = arguments ++ args))
//
//  def removeArguments(args: Set[String]): Gen[State] =
//    const(copy(arguments = arguments -- args))
//
//  def addParameters(ps: Set[String]): Gen[State] =
//    const(copy(parameters = parameters ++ ps))
//
//  def pushLeafCardinalityMultiplier(c: Cardinality): Gen[State] =
//    const(copy(leafCardinalityMultipliers = c +: leafCardinalityMultipliers))
//
//  def popLeafCardinalityMultiplier(): Gen[State] =
//    const(copy(leafCardinalityMultipliers = leafCardinalityMultipliers.tail))
//
//  def recordLabel(variable: String, label: String): Gen[State] = {
//    val newLabels = labelInfo(label) + LabelName(label)(pos)
//    const(copy(labelInfo = labelInfo.updated(variable, newLabels)))
//  }
//}

  /**
   * This generates random expressions and then uses SematicChecking to see if they are valid. This works,
   * but is inefficient and will miss lots of expressions for which it is harder to generate valid instances.
   */
  private def validExpression(availableSymbols: Seq[String], state: State, expressionGen: SemanticAwareAstGenerator => Gen[Expression]): Gen[WithState[Expression]] = {
    val semanticState = SemanticState(new ScopeLocation(Scope.empty.location(ScopeZipper)), state.semanticTable.types, ASTAnnotationMap.empty)

    for {
      expression <- expressionGen(new SemanticAwareAstGenerator(allowedVarNames = Some(availableSymbols)))
        .suchThat(e => {
          val errors = SemanticExpressionCheck.check(Results, e)(semanticState).errors
          if(errors.nonEmpty) println(errors)
          errors.isEmpty
        })
      parameters = expression.findByAllClass[Parameter].map(_.name)
      state <- state.addParameters(parameters.toSet)
    } yield {
      WithState(expression, state)
    }
  }

}
