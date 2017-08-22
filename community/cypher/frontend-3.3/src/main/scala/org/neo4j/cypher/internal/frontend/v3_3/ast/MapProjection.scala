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

import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3._

case class MapProjection(name: Variable, items: Seq[MapProjectionElement], outerScope: Scope = Scope.empty)(
    val position: InputPosition)
    extends Expression
    with SimpleTyping {
  protected def possibleTypes = CTMap

  override def semanticCheck(ctx: SemanticContext) =
    items.semanticCheck(ctx) chain
      super.semanticCheck(ctx) ifOkChain // We need to remember the scope to later rewrite this ASTNode
      recordCurrentScope

  def withOuterScope(outerScope: Scope) =
    copy(outerScope = outerScope)(position)
}

sealed trait MapProjectionElement extends SemanticCheckableWithContext with ASTNode

case class LiteralEntry(key: PropertyKeyName, exp: Expression)(val position: InputPosition)
    extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = exp.semanticCheck(ctx)
}

case class VariableSelector(id: Variable)(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = id.semanticCheck(ctx)
}

case class PropertySelector(id: Variable)(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
}

case class AllPropertiesSelector()(val position: InputPosition) extends MapProjectionElement {
  override def semanticCheck(ctx: SemanticContext) = SemanticCheckResult.success
}
