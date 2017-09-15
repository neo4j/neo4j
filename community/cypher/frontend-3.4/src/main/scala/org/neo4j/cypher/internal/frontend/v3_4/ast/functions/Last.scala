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

import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, TypeGenerator, ast}
import org.neo4j.cypher.internal.frontend.v3_4.ast.Function
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

case object Last extends Function with SemanticAnalysisTooling {
  def name = "last"

  override def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkArgs(invocation, 1) ifOkChain {
      expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
      specifyType(possibleInnerTypes(invocation.arguments.head), invocation)
    }

  private def possibleInnerTypes(expression: ast.Expression) : TypeGenerator = s =>
    (expression.types(s) constrain CTList(CTAny)).unwrapLists
}
