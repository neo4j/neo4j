/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.expressions.functions.DeterministicFunction.isFunctionDeterministic
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

object Expression {
  sealed trait SemanticContext

  object SemanticContext {
    case object Simple extends SemanticContext
    case object Results extends SemanticContext
  }

  val DefaultTypeMismatchMessageGenerator =
    (expected: String, existing: String) => s"expected $expected but was $existing"

  /**
   * An Accumulator for walking over an Expressions tree while keeping track of variables in scope.
   *
   * @param data the accumulated data
   * @param list a list of scopes, where each element is a Set of Variables in that scope.
   */
  final case class TreeAcc[A](data: A, list: List[Set[LogicalVariable]] = List.empty) {
    def mapData(f: A => A): TreeAcc[A] = copy(data = f(data))

    def inScope(variable: LogicalVariable): Boolean = list.exists(_.contains(variable))

    def pushScope(newVariable: LogicalVariable): TreeAcc[A] = pushScope(Set(newVariable))
    def pushScope(newVariables: Set[LogicalVariable]): TreeAcc[A] = copy(list = newVariables :: list)
    def popScope: TreeAcc[A] = copy(list = list.tail)
  }

  def mapExpressionHasPropertyReadDependency(mapEntity: LogicalVariable, mapExpression: Expression): Boolean =
    mapExpression match {
      case MapExpression(items) => items.exists {
          case (k, v) => v.subExpressions.exists {
              case LogicalProperty(LogicalVariable(entityName), propertyKey) =>
                entityName == mapEntity.name && propertyKey == k
              case _ => false
            }
        }
      case _ => false
    }

  def hasPropertyReadDependency(
    entity: LogicalVariable,
    expression: Expression,
    propertyKey: PropertyKeyName
  ): Boolean =
    expression.subExpressions.exists {
      case LogicalProperty(LogicalVariable(name), key) =>
        name == entity.name && key == propertyKey
      case _ =>
        false
    }
}

abstract class Expression extends ASTNode {

  self =>

  /**
   * Collects the immediate arguments to this Expression.
   */
  def arguments: Seq[Expression] = this.folder.treeFold(List.empty[Expression]) {
    case e: Expression if e != this =>
      acc => SkipChildren(acc :+ e)
  }

  /**
   * Collects all sub-expressions recursively .
   */
  def subExpressions: Seq[Expression] = this.folder.treeFold(List.empty[Expression]) {
    case e: Expression if e != this =>
      acc => TraverseChildren(acc :+ e)
  }

  /** 
   * All variables referenced from this expression or any of its children
   * that are not introduced inside this expression.
   */
  def dependencies: Set[LogicalVariable] =
    this.folder.treeFold(Expression.TreeAcc[Set[LogicalVariable]](Set.empty)) {
      case scope: ScopeExpression =>
        acc =>
          val newDependencies = scope.dependencies.filterNot(acc.inScope)
          val newAcc = acc.mapData(_ ++ newDependencies)
          SkipChildren(newAcc)
      case id: LogicalVariable => acc => {
          val newAcc = if (acc.inScope(id)) acc else acc.mapData(_ + id)
          TraverseChildren(newAcc)
        }
    }.data

  /** 
   * All (free) occurrences of variable in this expression or any of its children
   * (i.e. excluding occurrences referring to shadowing redefinitions of variable).
   *
   * This method must not be called before SemanticAnalysis has passed.
   * Otherwise an [[ExpressionWithComputedDependencies]] will not have computed
   * its dependencies.
   *
   * Setting `skipScopeExpression` to true will skip checking [[ScopeExpression]]
   */
  def occurrences(variable: LogicalVariable, skipScopeExpression: Boolean = false): Set[Ref[Variable]] = {
    def visitOccurrence(
      acc: Expression.TreeAcc[Set[Ref[Variable]]],
      occurrence: Variable
    ): Expression.TreeAcc[Set[Ref[Variable]]] = {
      if (occurrence.name != variable.name || acc.inScope(occurrence)) acc
      else acc.mapData(_ + Ref(occurrence))
    }

    this.folder.treeFold(Expression.TreeAcc[Set[Ref[Variable]]](Set.empty)) {
      case scope: ScopeExpression if !skipScopeExpression =>
        acc =>
          val accStep1 = scope match {
            case ewcd: ExpressionWithComputedDependencies =>
              // Also look for occurrences in scope dependencies.
              // No need to look in introducedVariables, as they will always shadow and can't be an occurrence of the same
              // variable.
              val scopeDependencies = ewcd.scopeDependencies.asInstanceOf[Set[Variable]]
              scopeDependencies.foldLeft(acc)(visitOccurrence)
            case _ => acc
          }
          val newAcc = accStep1.pushScope(scope.introducedVariables)
          TraverseChildrenNewAccForSiblings(newAcc, _.popScope)
      case occurrence: Variable =>
        acc => TraverseChildren(visitOccurrence(acc, occurrence))
    }.data
  }

  /**
   * Replaces all occurrences of a variable in this Expression by the given replacement.
   * This takes into account scoping and does not replace other Variables with the same name
   * in nested inner scopes.
   *
   * This method must not be called before SemanticAnalysis has passed.
   * Otherwise an [[ExpressionWithComputedDependencies]] will not have computed
   * its dependencies.
   *
   * @param variable    the variable to replace
   * @param replacement the replacement expression
   * @param skipExpressionsWithComputedDependencies allows skipping replacement for [[ExpressionWithComputedDependencies]]
   * @return this expression with `variable` replaced by `replacement`.
   */
  def replaceAllOccurrencesBy(
    variable: LogicalVariable,
    replacement: => Expression,
    skipExpressionsWithComputedDependencies: Boolean = false
  ): Expression = {
    val occurrencesToReplace = occurrences(variable, skipExpressionsWithComputedDependencies)
    self.endoRewrite(bottomUp(Rewriter.lift {
      case occurrence: Variable if occurrencesToReplace(Ref(occurrence))                        => replacement
      case ewcd: ExpressionWithComputedDependencies if !skipExpressionsWithComputedDependencies =>
        // Rewrite scope dependencies.
        // No need to rewrite introducedVariables, as they will always shadow and can't be an occurrence of the same
        // variable.
        val newScopeDependencies = ewcd.scopeDependencies.map {
          case occurrence: Variable if occurrencesToReplace(Ref(occurrence)) =>
            replacement match {
              case lv: LogicalVariable => lv
              case _ => throw new IllegalStateException(
                  s"Cannot replace the dependency on variable $occurrence with a non-variable expression"
                )
            }
          case x => x
        }
        ewcd.withComputedScopeDependencies(newScopeDependencies)
    }))
  }

  /**
   * Return true is this expression contains an aggregating expression.
   */
  def containsAggregate: Boolean = this.folder.treeExists {
    case IsAggregate(_) => true
  }

  /**
   * Returns the first encountered aggregate expression, or None if none existed.
   */
  def findAggregate: Option[Expression] = this.folder.treeFind[Expression] {
    case IsAggregate(_) => true
  }

  /**
   * Return true if this expression contains a scope expression.
   */
  def containsScopeExpression: Boolean = this.folder.treeExists {
    case _: ScopeExpression => true
  }

  // Note! Will not consider ResolvedFunctionInvocation which can be non deterministic.
  def isDeterministic: Boolean = this.folder.treeForall {
    case f: FunctionInvocation => isFunctionDeterministic(f.function)
    case _                     => true
  }

  /**
   * 
   * @return `true` if expression is constant and doesn't require the incoming row to be evaluated.
   */
  def isConstantForQuery: Boolean

  /**
   * An expression is simple if it's deterministic and cheap to evaluate.
   * It's important to know when re-ordering clauses.
   */
  def isSimple: Boolean =
    subExpressions.isEmpty
}

/**
 * Signifies that this expression doesn't have to be coerced if used as a predicate
 */
trait BooleanExpression extends Expression
