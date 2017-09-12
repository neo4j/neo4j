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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.frontend.v3_4.symbols.CTString
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, SemanticCheckable, SemanticChecking}

final case class GraphUrl(url: Either[Parameter, StringLiteral])(val position: InputPosition)
  extends ASTNode with SemanticCheckable with SemanticChecking {

  override def semanticCheck: SemanticCheck = url match {
    case Left(parameter) =>
      parameter.semanticCheck(Expression.SemanticContext.Simple) chain parameter.expectType(CTString.covariant)

    case Right(literal) =>
      literal.semanticCheck(Expression.SemanticContext.Simple) chain literal.expectType(CTString.covariant)
  }
}
