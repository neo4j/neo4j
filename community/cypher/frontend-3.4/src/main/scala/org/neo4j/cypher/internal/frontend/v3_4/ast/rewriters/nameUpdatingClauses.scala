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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Create, CreateUnique, Merge}
import org.neo4j.cypher.internal.v3_4.expressions.Expression

case object nameUpdatingClauses extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val findingRewriter: Rewriter = Rewriter.lift {
    case createUnique@CreateUnique(pattern) =>
      val rewrittenPattern = pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      createUnique.copy(pattern = rewrittenPattern)(createUnique.position)

    case create@Create(pattern) =>
      val rewrittenPattern = pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      create.copy(pattern = rewrittenPattern)(create.position)

    case merge@Merge(pattern, _, _) =>
      val rewrittenPattern = pattern.endoRewrite(nameAllPatternElements.namingRewriter)
      merge.copy(pattern = rewrittenPattern)(merge.position)
  }

  private val instance = bottomUp(findingRewriter, _.isInstanceOf[Expression])
}
