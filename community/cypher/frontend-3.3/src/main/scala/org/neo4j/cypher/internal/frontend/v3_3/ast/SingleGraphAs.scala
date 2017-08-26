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

sealed trait SingleGraphAs extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  def as: Option[Variable]

  def withNewName(newName: Variable): SingleGraphAs

  override def semanticCheck: SemanticCheck =
    inner chain checkAlias


  protected def inner: SemanticCheck

  protected def checkAlias: SemanticCheck =
    as.foldSemanticCheck(v => v.declareGraph)
}

sealed trait BoundGraphAs extends SingleGraphAs

final case class SourceGraphAs(as: Option[Variable])(val position: InputPosition) extends BoundGraphAs {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

final case class TargetGraphAs(as: Option[Variable])(val position: InputPosition) extends BoundGraphAs {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

final case class GraphAs(ref: Variable, as: Option[Variable])(val position: InputPosition)
  extends BoundGraphAs {

  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = ref.ensureDefined() chain ref.expectType(CTGraphRef.covariant)
  override protected def checkAlias: SemanticCheck = as match {
    case Some(newRef) if newRef.name != ref.name => super.checkAlias
    case _ => SemanticCheckResult.success
  }
}

sealed trait NewGraphAs extends SingleGraphAs

final case class GraphOfAs(of: Pattern, as: Option[Variable])(val position: InputPosition)
  extends NewGraphAs {

  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = of.semanticCheck(Pattern.SemanticContext.Create)
}

final case class GraphAtAs(at: GraphUrl, as: Option[Variable])(val position: InputPosition)
  extends NewGraphAs {

  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = at.semanticCheck
}

