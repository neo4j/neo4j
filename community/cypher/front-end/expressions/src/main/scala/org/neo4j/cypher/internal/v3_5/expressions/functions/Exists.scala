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
package org.neo4j.cypher.internal.v3_5.expressions.functions

import org.neo4j.cypher.internal.v3_5.expressions.{Expression, FunctionInvocation}
import org.neo4j.cypher.internal.v3_5.util.InputPosition

case object Exists extends Function {

  private val functionName = asFunctionName(InputPosition.NONE)

  def name = "EXISTS"

  def unapply(arg: Expression): Option[Expression] =
    arg match {
      case FunctionInvocation(_, functionName, _, args) => Some(args.head)
      case _ => None
    }

  def apply(arg: Expression): FunctionInvocation =
    FunctionInvocation(arg, functionName)
}
