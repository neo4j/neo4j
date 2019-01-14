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
package org.neo4j.cypher.internal.frontend.v3_4.ast.conditions

import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.Condition
import org.neo4j.cypher.internal.v3_4.expressions.{Equals, Expression, FunctionInvocation, Property}
import org.neo4j.cypher.internal.v3_4.functions

case object normalizedEqualsArguments extends Condition {
  def apply(that: Any): Seq[String] = {
    val equals = collectNodesOfType[Equals].apply(that)
    equals.collect {
      case eq@Equals(expr, Property(_,_)) if !expr.isInstanceOf[Property] && notIdFunction(expr) =>
        s"Equals at ${eq.position} is not normalized: $eq"
      case eq@Equals(expr, func@FunctionInvocation(_, _, _, _)) if isIdFunction(func) && notIdFunction(expr) =>
        s"Equals at ${eq.position} is not normalized: $eq"
    }
  }

  private def isIdFunction(func: FunctionInvocation) = func.function == functions.Id

  private def notIdFunction(expr: Expression) =
    !expr.isInstanceOf[FunctionInvocation] || !isIdFunction(expr.asInstanceOf[FunctionInvocation])

  override def name: String = productPrefix
}
