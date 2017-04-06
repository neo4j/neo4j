/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.helpers

import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.ast._

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
