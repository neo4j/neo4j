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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckableExpression
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.util.InputPosition

case class CoerceToPredicate(inner: Expression) extends BooleanExpression with SemanticCheckableExpression {

  override def semanticCheck(ctx: SemanticContext): SemanticCheck = SemanticCheck.success

  override def asCanonicalStringVal: String = {
    s"CoerceToPredicate(${inner.asCanonicalStringVal})"
  }

  override def position: InputPosition = InputPosition.NONE

  // We are breaking the implicit assumption that every ASTNode has a position as second parameter list.
  // That is why, we need to adjust the dup method's behaviour
  override def dup(children: Seq[AnyRef]): this.type =
    CoerceToPredicate(children.head.asInstanceOf[Expression]).asInstanceOf[this.type]

  override def isConstantForQuery: Boolean = inner.isConstantForQuery
}
