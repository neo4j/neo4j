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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement.ExtractParameterRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement.LiteralReplacements
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.symbols.CTString

object sensitiveLiteralReplacement {

  private val sensitiveliteralMatcher
    : PartialFunction[Any, LiteralReplacements => Foldable.FoldingBehavior[LiteralReplacements]] = {
    case l: SensitiveStringLiteral =>
      acc =>
        if (acc.contains(l)) {
          SkipChildren(acc)
        } else {
          val parameter =
            new AutoExtractedParameter(s"  AUTOSTRING${acc.size}", CTString)(l.position) with SensitiveAutoParameter
          SkipChildren(acc + (l -> parameter))
        }
  }

  def apply(term: ASTNode): (Rewriter, Map[AutoExtractedParameter, Expression]) = {
    val replaceableLiterals = term.folder.treeFold(IdentityMap.empty: LiteralReplacements)(sensitiveliteralMatcher)
    val extractedParams = replaceableLiterals.map {
      case (e, parameter) => parameter -> e
    }
    (ExtractParameterRewriter(replaceableLiterals), extractedParams)
  }
}
