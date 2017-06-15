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
package org.neo4j.cypher.internal.frontend.v3_3.helpers

import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.ast._

/*
 * Calculates how to transform a pattern (a)-[:R1:R2...]->() to getDegree call
 * of that very pattern.
 */
object calculateUsingGetDegree {

  def apply(expr: Expression, node: Variable, types: Seq[RelTypeName], dir: SemanticDirection): Expression = {
      types
        .map(typ => GetDegree(node.copyId, Some(typ), dir)(typ.position))
        .reduceOption[Expression](Add(_, _)(expr.position))
        .getOrElse(GetDegree(node, None, dir)(expr.position))
    }
}
