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
package org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, bottomUp}

case object nameAllGraphs extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  // TODO: resolve source and target graph
  private val rewriter = Rewriter.lift {
    case item: SourceGraphItem => item
    case item: TargetGraphItem => item
    case graphItem@GraphRefAliasItem(alias@GraphRefAlias(ref, None)) =>
      graphItem.copy(alias = alias.copy(as = Some(ref.name))(alias.position))(graphItem.position)
    case graphItem: SingleGraphItem if graphItem.as.isEmpty =>
      val pos = graphItem.position.bumped()
      graphItem.withNewName(Variable(UnNamedNameGenerator.name(pos))(pos))
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])
}
