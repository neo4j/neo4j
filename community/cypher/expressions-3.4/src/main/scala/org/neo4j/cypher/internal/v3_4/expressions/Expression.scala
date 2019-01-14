/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4._

import scala.collection.immutable.Stack

object Expression {
  sealed trait SemanticContext
  object SemanticContext {
    case object Simple extends SemanticContext
    case object Results extends SemanticContext
  }

  val DefaultTypeMismatchMessageGenerator = (expected: String, existing: String) => s"expected $expected but was $existing"

  final case class TreeAcc[A](data: A, stack: Stack[Set[LogicalVariable]] = Stack.empty) {
    def mapData(f: A => A): TreeAcc[A] = copy(data = f(data))

    def inScope(variable: LogicalVariable) = stack.exists(_.contains(variable))
    def variablesInScope: Set[LogicalVariable] = stack.toSet.flatten

    def pushScope(newVariable: LogicalVariable): TreeAcc[A] = pushScope(Set(newVariable))
    def pushScope(newVariables: Set[LogicalVariable]): TreeAcc[A] = copy(stack = stack.push(newVariables))
    def popScope: TreeAcc[A] = copy(stack = stack.pop)
  }

  def mapExpressionHasPropertyReadDependency(mapEntityName: String, mapExpression: Expression): Boolean =
    mapExpression match {
      case MapExpression(items) => items.exists {
        case (k, v) => v.subExpressions.exists {
          case LogicalProperty(LogicalVariable(entityName), propertyKey) =>
            entityName == mapEntityName && propertyKey == k
          case _ => false
        }
      }
      case _ => false
    }

  def hasPropertyReadDependency(entityName: String, expression: Expression, propertyKey: PropertyKeyName): Boolean =
    expression.subExpressions.exists {
      case LogicalProperty(LogicalVariable(name), key) =>
        name == entityName && key == propertyKey
      case _ =>
        false
    }
}

abstract class Expression extends ASTNode {

  self =>

  import Expression.TreeAcc

  def arguments: Seq[Expression] = this.treeFold(List.empty[Expression]) {
    case e: Expression if e != this =>
      acc => (acc :+ e, None)
  }

  // Collects all sub-expressions recursively
  def subExpressions: Seq[Expression] = this.treeFold(List.empty[Expression]) {
    case e: Expression if e != this =>
      acc => (acc :+ e, Some(identity))
  }

  // All variables referenced from this expression or any of its children
  // that are not introduced inside this expression
  def dependencies: Set[LogicalVariable] =
    this.treeFold(TreeAcc[Set[LogicalVariable]](Set.empty)) {
      case scope: ScopeExpression =>
        acc =>
          val newAcc = acc.pushScope(scope.introducedVariables)
          (newAcc, Some((x) => x.popScope))
      case id: LogicalVariable => acc => {
        val newAcc = if (acc.inScope(id)) acc else acc.mapData(_ + id)
        (newAcc, Some(identity))
      }
    }.data

  // All (free) occurrences of variable in this expression or any of its children
  // (i.e. excluding occurrences referring to shadowing redefinitions of variable)
  def occurrences(variable: LogicalVariable): Set[Ref[Variable]] =
    this.treeFold(TreeAcc[Set[Ref[Variable]]](Set.empty)) {
      case scope: ScopeExpression => {
        case acc =>
          val newAcc = acc.pushScope(scope.introducedVariables)
          (newAcc, Some((x) => x.popScope))
      }
      case occurrence: Variable if occurrence.name == variable.name => acc => {
        val newAcc = if (acc.inScope(occurrence)) acc else acc.mapData(_ + Ref(occurrence))
        (newAcc, Some(identity))
      }
    }.data

  def copyAndReplace(variable: LogicalVariable) = new {
    def by(replacement: => Expression): Expression = {
      val replacedOccurences = occurrences(variable)
      self.endoRewrite(bottomUp(Rewriter.lift {
        case occurrence: Variable if replacedOccurences(Ref(occurrence)) => replacement
      }))
    }
  }

  // List of child expressions together with any of its dependencies introduced
  // by any of its parent expressions (where this expression is the root of the tree)
  def inputs: Seq[(Expression, Set[LogicalVariable])] =
    this.treeFold(TreeAcc[Seq[(Expression, Set[LogicalVariable])]](Seq.empty)) {
      case scope: ScopeExpression=>
        acc =>
          val newAcc = acc.pushScope(scope.introducedVariables)
            .mapData(pairs => pairs :+ (scope -> acc.variablesInScope))
          (newAcc, Some((x) => x.popScope))

      case expr: Expression =>
        acc =>
          val newAcc = acc.mapData(pairs => pairs :+ (expr -> acc.variablesInScope))
          (newAcc, Some(identity))
    }.data

  /**
    * Return true is this expression contains an aggregating expression.
    */
  def containsAggregate: Boolean = this.treeExists {
    case IsAggregate(_) => true
  }

  /**
    * Returns the first encountered aggregate expression, or None if none existed.
    */
  def findAggregate:Option[Expression] = this.treeFind[Expression] {
    case IsAggregate(_) => true
  }
}
