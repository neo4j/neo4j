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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTGraphRef

final case class GraphAlias(ref: Variable, as: Option[Variable])(val position: InputPosition)
  extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  def withNewName(newName: Variable) = copy(as = Some(newName))(position)

  override def semanticCheck: SemanticCheck =
    ref.semanticCheck(Expression.SemanticContext.Simple) chain checkAlias chain as.expectType(CTGraphRef.covariant)

  def checkAlias: SemanticCheck =
    as match {
      case Some(v) if v.name != ref.name =>
        as.map(_.declareGraph: SemanticCheck).getOrElse(SemanticCheckResult.success)
      case _ => SemanticCheckResult.success
    }
}
