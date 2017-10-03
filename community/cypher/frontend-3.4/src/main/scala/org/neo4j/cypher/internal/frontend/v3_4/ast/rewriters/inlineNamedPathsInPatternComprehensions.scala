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
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.aux.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions._

case object inlineNamedPathsInPatternComprehensions extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case expr @ PatternComprehension(Some(path), pattern, predicate, projection, _) =>
      val patternElement = pattern.element
      expr.copy(
        namedPath = None,
        predicate = predicate.map(_.inline(path, patternElement)),
        projection = projection.inline(path, patternElement)
      )(expr.position)
  })

  private implicit final class InliningExpression(val expr: Expression) extends AnyVal {
    def inline(path: Variable, patternElement: PatternElement) =
      expr.copyAndReplace(path) by {
        PathExpression(projectNamedPaths.patternPartPathExpression(patternElement))(expr.position)
      }
  }

  override def apply(v: AnyRef): AnyRef = instance(v)
}
