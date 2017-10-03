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
import org.neo4j.cypher.internal.frontend.v3_4.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.v3_4.expressions.{NodePattern, RelationshipPattern, Variable}

case object nameAllPatternElements extends Rewriter {

  override def apply(in: AnyRef): AnyRef = namingRewriter.apply(in)

  val namingRewriter: Rewriter = bottomUp(Rewriter.lift {
    case pattern: NodePattern if pattern.variable.isEmpty =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)

    case pattern: RelationshipPattern if pattern.variable.isEmpty  =>
      val syntheticName = UnNamedNameGenerator.name(pattern.position.bumped())
      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)
  })
}
