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

import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentOrder
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.expressions.functions.DeterministicFunction
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.util.InputPosition

import java.util.Locale

object FunctionInvocation {

  sealed trait ArgumentOrder
  case object ArgumentUnordered extends ArgumentOrder
  case object ArgumentAsc extends ArgumentOrder
  case object ArgumentDesc extends ArgumentOrder

  def apply(name: FunctionName, argument: Expression)(position: InputPosition): FunctionInvocation =
    FunctionInvocation(
      name,
      distinct = false,
      IndexedSeq(argument)
    )(position)

  def apply(left: Expression, name: FunctionName, right: Expression): FunctionInvocation =
    FunctionInvocation(
      name,
      distinct = false,
      IndexedSeq(left, right)
    )(name.position)

  def apply(expression: Expression, name: FunctionName): FunctionInvocation =
    FunctionInvocation(
      name,
      distinct = false,
      IndexedSeq(expression)
    )(name.position)
}

/**
 * Deterministic function invocation, see [[DeterministicFunction]].
 */
object DeterministicFunctionInvocation {

  def unapply(expr: FunctionInvocation): Option[FunctionInvocation] = {
    expr.function match {
      case DeterministicFunction(_) => Some(expr)
      case _                        => None
    }
  }
}

case class FunctionInvocation(
  functionName: FunctionName,
  distinct: Boolean,
  args: IndexedSeq[Expression],
  order: ArgumentOrder = ArgumentUnordered,
  calledFromUseClause: Boolean = false
)(val position: InputPosition) extends Expression {
  val name: String = (functionName.namespace.parts :+ functionName.name).mkString(".")

  val function: functions.Function =
    functions.Function.lookup.getOrElse(name.toLowerCase(Locale.ROOT), UnresolvedFunction)

  val isOrdered: Boolean = order != ArgumentUnordered

  def withOrder(o: ArgumentOrder): FunctionInvocation = copy(order = o)(position)

  def needsToBeResolved: Boolean = function match {
    case UnresolvedFunction => true
    case _                  => false
  }

  override def asCanonicalStringVal = s"$name(${args.map(_.asCanonicalStringVal).mkString(",")})"

  override def isConstantForQuery: Boolean =
    !needsToBeResolved &&
      isDeterministic &&
      function != functions.File &&
      function != functions.Linenumber &&
      args.forall(_.isConstantForQuery)

  override def isSimple: Boolean =
    isDeterministic && subExpressions.isEmpty
}

case class FunctionName(namespace: Namespace, name: String)(val position: InputPosition) extends SymbolicName {

  override def equals(x: Any): Boolean = x match {
    case FunctionName(otherNamespace, otherName) =>
      otherNamespace == namespace && otherName.toLowerCase(Locale.ROOT) == name.toLowerCase(Locale.ROOT)
    case _ => false
  }
  override def hashCode = name.toLowerCase(Locale.ROOT).hashCode
}

object FunctionName {

  def apply(name: String)(position: InputPosition): FunctionName = {
    FunctionName(Namespace()(position), name)(position)
  }
}
