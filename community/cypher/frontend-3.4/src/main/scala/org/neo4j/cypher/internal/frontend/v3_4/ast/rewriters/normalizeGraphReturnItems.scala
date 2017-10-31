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

import org.neo4j.cypher.internal.util.v3_4.{FreshIdNameGenerator, Rewriter, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, Variable}

case object normalizeGraphReturnItems extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case item: SourceGraphAs => item
    case item: TargetGraphAs => item
    case graphItem@GraphAs(ref, None, _) =>
      graphItem.copy(as = Some(Variable(ref.name)(ref.position)))(graphItem.position)
    case graphItem: SingleGraphAs if graphItem.as.isEmpty =>
      val pos = graphItem.position.bumped()
      graphItem.withNewName(Variable(FreshIdNameGenerator.name(pos))(pos)).asGenerated
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])
}
