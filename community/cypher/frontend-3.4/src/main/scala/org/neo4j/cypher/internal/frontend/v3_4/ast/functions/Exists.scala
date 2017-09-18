/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast.functions

import org.neo4j.cypher.internal.frontend.v3_4.ast.{ContainerIndex, Function}
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticError, ast}

case object Exists extends Function {
  def name = "EXISTS"

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation) =
    checkArgs(invocation, 1) ifOkChain {
      invocation.arguments.head.expectType(CTAny.covariant) chain
        (invocation.arguments.head match {
          case _: ast.Property => None
          case _: ast.PatternExpression => None
          case _: ContainerIndex => None
          case e =>
            Some(SemanticError(s"Argument to ${invocation.name}(...) is not a property or pattern", e.position, invocation.position))
        })
    } chain invocation.specifyType(CTBoolean)
}
