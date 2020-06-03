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

import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.IdentityMap
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType

object parameterValueTypeReplacement {

  case class ParameterValueTypeReplacement(parameter: Parameter, value: AnyRef)
  type ParameterValueTypeReplacements = IdentityMap[Expression, ParameterValueTypeReplacement]

  case class ExtractParameterRewriter(replaceableParameters: ParameterValueTypeReplacements) extends Rewriter {
    def apply(that: AnyRef): AnyRef = rewriter.apply(that)

    private val rewriter: Rewriter = bottomUp(Rewriter.lift {
      case p: Parameter if replaceableParameters.isDefinedAt(p) => replaceableParameters(p).parameter
    })
  }

  private def rewriteParameterValueTypes(term: ASTNode, paramTypes: Map[String, CypherType]) = {
    val replaceableParameters = term.treeFold(IdentityMap.empty: ParameterValueTypeReplacements){
      case p@ExplicitParameter(_, CTAny) =>
        acc =>
          if (acc.contains(p)) SkipChildren(acc) else {
            val cypherType = paramTypes.getOrElse(p.name, CTAny)
            SkipChildren(acc + (p -> ParameterValueTypeReplacement(ExplicitParameter(p.name, cypherType)(p.position), p.name)))
          }
    }
    ExtractParameterRewriter(replaceableParameters)
  }

  def apply(term: ASTNode, parameterTypeMapping: Map[String, CypherType]): Rewriter =
    rewriteParameterValueTypes(term, parameterTypeMapping)
}
