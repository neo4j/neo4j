/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SensitiveAutoParameter
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement.ExtractParameterRewriter
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement.LiteralReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement.LiteralReplacements
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.symbols.CTString

object sensitiveLiteralReplacement {

  private val sensitiveliteralMatcher: PartialFunction[Any, LiteralReplacements => (LiteralReplacements, Option[LiteralReplacements => LiteralReplacements])] = {
    case l: SensitiveStringLiteral =>
      acc =>
        if (acc.contains(l)) {
          (acc, None)
        } else {
          val parameter = new Parameter(s"  AUTOSTRING${acc.size}", CTString)(l.position) with SensitiveAutoParameter
          (acc + (l -> LiteralReplacement(parameter, l.value)), None)
        }
  }

  def apply(term: ASTNode): (Rewriter, Map[String, Any]) = {
    val replaceableLiterals = term.treeFold(IdentityMap.empty: LiteralReplacements)(sensitiveliteralMatcher)

    val extractedParams: Map[String, AnyRef] = replaceableLiterals.map {
      case (_, LiteralReplacement(parameter, value)) => (parameter.name, value)
    }

    (ExtractParameterRewriter(replaceableLiterals), extractedParams)
  }
}
