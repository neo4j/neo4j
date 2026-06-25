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

import org.neo4j.cypher.internal.util.FunctionName

/**
 * Common parent for any AST node that represents a function call, whether or not the
 * call has been resolved against a function signature.
 *
 * Implementations:
 *  - [[FunctionInvocation]] — pre-resolution or built-in.
 *  - org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation — resolved user-defined and temporals.
 */
trait FunctionInvocationLike extends Expression {
  def functionName: FunctionName
  def callArguments: Seq[Expression]
  def isAggregate: Boolean
  def isUserDefined: Boolean
  def asUnresolvedFunction: FunctionInvocation

  /**
   * True if and only if this call resolves to a built-in function — a compiler built-in
   * ([[FunctionInvocation]]) or a registered built-in user function
   * (org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation) — and is not
   * shadowed by a user-defined function. Requires function resolution to have run.
   */
  def isBuiltIn: Boolean
}
