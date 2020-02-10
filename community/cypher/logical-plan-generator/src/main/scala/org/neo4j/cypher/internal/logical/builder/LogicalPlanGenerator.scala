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
package org.neo4j.cypher.internal.logical.builder

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
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext.Results
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.builder.LogicalPlanGenerator.State
import org.neo4j.cypher.internal.logical.builder.LogicalPlanGenerator.WithState
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
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.scalacheck.Gen
import org.scalacheck.Gen.choose
import org.scalacheck.Gen.chooseNum
import org.scalacheck.Gen.const
import org.scalacheck.Gen.lzy
import org.scalacheck.Gen.oneOf
import org.scalacheck.Gen.sized

object LogicalPlanGenerator extends AstConstructionTestSupport {
  case class WithState[+T](x: T, state: State)

  object State {
    def apply(): State = this (SemanticTable(), Set.empty, 0, Set.empty)
  }

  /**
   * Accumulated state while generating logical plans
   * @param semanticTable the semantic table
   * @param arguments arguments, which are valid at this point of generation.
   * @param varCount the amount of generated distinct variables
   * @param parameters the generated parameter names
   */
  case class State(semanticTable: SemanticTable, arguments: Set[String], varCount: Int, parameters: Set[String]) {

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
     *       WithState(Expand(source, from, dir, relTypes, to, rel)(idGen), state)
     *     }
     * }}}
     *
     * Like this we shadow `state` with a new variable called `state` each time we obtain a new state.
     * This makes it quite easy to modify these comprehensions without having to change a lot of references that would now have to point to the new state
     * and are easy to miss.
     */

    def newVariable: Gen[WithState[String]] = {
      val name = s"var${varCount}"
      const(WithState(name, State(semanticTable, arguments, varCount + 1, parameters)))
    }

    def newNode(name: String): Gen[State] = {
      const(State(semanticTable.addNode(varFor(name)), arguments, varCount, parameters))
    }

    def newRelationship(name: String): Gen[State] = {
      const(State(semanticTable.addRelationship(varFor(name)), arguments, varCount, parameters))
    }

    def declareTypeAny(name: String): Gen[State] = {
      const(State(semanticTable.copy(types = semanticTable.types.updated(varFor(name), ExpressionTypeInfo(CTAny.invariant, None))), arguments, varCount, parameters))
    }

    def addArguments(args: Set[String]): Gen[State] = {
      const(State(semanticTable, arguments ++ args, varCount, parameters))
    }

    def removeArguments(args: Set[String]): Gen[State] = {
      const(State(semanticTable, arguments -- args, varCount, parameters))
    }

    def addParameters(ps: Set[String]): Gen[State] = {
      const(State(semanticTable, arguments, varCount, parameters ++ ps))
    }
  }
}

/**
 * A generator of random logical plans, with the ambition to generate only valid plans.
 * @param labels The labels that exist in a graph that the plans can be executed again.
 * @param relTypes The relationship types that exist in a graph that the plans can be executed again.
 */
class LogicalPlanGenerator(labels: Seq[String], relTypes: Seq[String]) extends AstConstructionTestSupport {

  private val idGen: IdGen = new SequentialIdGen()

  /**
   * Main entry point to obtain logical plans and associated state.
   */
  def logicalPlan: Gen[WithState[LogicalPlan]] = {
    for {
      WithState(plan, state) <- innerLogicalPlan(State())
    } yield {
      WithState(ProduceResult(plan, plan.availableSymbols.toSeq)(idGen), state)
    }
  }

  def innerLogicalPlan(state: State): Gen[WithState[LogicalPlan]] = oneOf[WithState[LogicalPlan]](
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
  )

  // Leaf Plans

  def allNodesScan(state: State): Gen[WithState[AllNodesScan]] = {
    for {
      WithState(node, state) <- state.newVariable
      state <- state.newNode(node)
    } yield WithState(AllNodesScan(node, state.arguments)(idGen), state)
  }

  def nodeByLabelScan(state: State): Gen[WithState[NodeByLabelScan]] =
    for {
      labelName <- label
      WithState(node, state) <- state.newVariable
      state <- state.newNode(node)
    } yield {
      WithState(NodeByLabelScan(node, labelName, state.arguments)(idGen), state)
    }

  def argument(state: State): Gen[WithState[Argument]] = {
    const(WithState(Argument(state.arguments)(idGen), state))
  }

  // One child plans

  def expand(state: State): Gen[WithState[Expand]] = {
    for {
      WithState(source, state) <- innerLogicalPlan(state).suchThat{ case WithState(plan, state) => plan.availableSymbols.exists(v => state.semanticTable.isNode(v))}
      from <- oneOf(source.availableSymbols.toSeq).suchThat(name => state.semanticTable.isNode(varFor(name)))
      dir <- semanticDirection
      relTypes <- relTypeNames
      WithState(to, state) <- state.newVariable
      state <- state.newNode(to)
      WithState(rel, state) <- state.newVariable
      state <- state.newRelationship(rel)
    } yield {
      WithState(Expand(source, from, dir, relTypes, to, rel)(idGen), state)
    }
  }

  def skip(state: State): Gen[WithState[Skip]] = {
    for {
      WithState(source, state) <- innerLogicalPlan(state)
      count <- chooseNum(0, Long.MaxValue, 1)
    } yield WithState(Skip(source, literalInt(count))(idGen), state)
  }

  def limit(state: State): Gen[WithState[Limit]] = {
    for {
      WithState(source, state) <- innerLogicalPlan(state)
      count <- chooseNum(0, Long.MaxValue, 1)
      ties <- if (source.isInstanceOf[Sort] && count == 1) oneOf(DoNotIncludeTies, IncludeTies) else const(DoNotIncludeTies)
    } yield WithState(Limit(source, literalInt(count), ties)(idGen), state)
  }

  def projection(state: State): Gen[WithState[Projection]] = {
    for {
      WithState(source, state) <- innerLogicalPlan(state)
      WithState(map, state) <- projectionList(state, source.availableSymbols.toSeq, _._expression)
    } yield WithState(Projection(source, map)(idGen), state)
  }

  private def projectionList(state: State, availableSymbols: Seq[String], expressionGen: SemanticAwareAstGenerator => Gen[Expression], minSize: Int = 0): Gen[WithState[Map[String, Expression]]] = {
    sized(s => choose(minSize, s max minSize)).flatMap { n =>
      (0 until n).foldLeft(const(WithState(Map.empty[String, Expression], state))) { (prevGen, _) =>
        for {
          WithState(map, state) <- prevGen
          WithState(name, state) <- state.newVariable
          WithState(xpr, state) <- validExpression(availableSymbols, state, expressionGen)
          state <- state.declareTypeAny(name)
        } yield {
          WithState(map + (name -> xpr), state)
        }
      }
    }
  }

  def aggregation(state: State): Gen[WithState[Aggregation]] = {
    for {
      WithState(source, state) <- innerLogicalPlan(state)
      WithState(groupingExpressions, state) <- projectionList(state, source.availableSymbols.toSeq, _._expression)
      WithState(aggregatingExpressions, state) <- projectionList(state, source.availableSymbols.toSeq, _.aggregationFunctionInvocation, minSize = 1)
    } yield WithState(Aggregation(source, groupingExpressions, aggregatingExpressions)(idGen), state)
  }

  // Two child plans

  def apply(state: State): Gen[WithState[Apply]] = {
    for {
      WithState(left, state) <- innerLogicalPlan(state)
      state <- state.addArguments(left.availableSymbols)
      WithState(right, state) <- innerLogicalPlan(state)
      state <- state.removeArguments(left.availableSymbols)
    } yield WithState(Apply(left, right)(idGen), state)
  }

  def cartesianProduct(state: State): Gen[WithState[CartesianProduct]] = {
    for {
      WithState(left, state) <- innerLogicalPlan(state)
      WithState(right, state) <- innerLogicalPlan(state)
    } yield WithState(CartesianProduct(left, right)(idGen), state)
  }

  // Other stuff

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
