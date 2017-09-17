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

import org.neo4j.cypher.internal.apa.v3_4.{Rewriter, topDown}
import org.neo4j.cypher.internal.v3_4.expressions._

case object normalizeComparisons extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case c@NotEquals(lhs, rhs) =>
      NotEquals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@Equals(lhs, rhs) =>
      Equals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@LessThan(lhs, rhs) =>
      LessThan(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@LessThanOrEqual(lhs, rhs) =>
      LessThanOrEqual(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@GreaterThan(lhs, rhs) =>
      GreaterThan(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@GreaterThanOrEqual(lhs, rhs) =>
      GreaterThanOrEqual(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@InvalidNotEquals(lhs, rhs) =>
      InvalidNotEquals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
  })
}
