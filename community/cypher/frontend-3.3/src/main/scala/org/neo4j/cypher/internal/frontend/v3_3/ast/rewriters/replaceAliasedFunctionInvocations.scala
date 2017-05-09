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
package org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_3.ast.{FunctionInvocation, FunctionName}
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, bottomUp}

import scala.collection.immutable.TreeMap

case object replaceAliasedFunctionInvocations extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  /*
   * These are historical names for functions. They are all subject to removal in an upcoming major release.
   */
  val aliases: Map[String, String] = TreeMap("toInt" -> "toInteger",
                                             "upper" -> "toUpper",
                                             "lower" -> "toLower",
                                             "rels" -> "relationships")(CaseInsensitiveOrdered)

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case func@FunctionInvocation(_, f@FunctionName(name), _, _) if aliases.get(name).nonEmpty =>
      func.copy(functionName = FunctionName(aliases(name))(f.position))(func.position)
  })

}

object CaseInsensitiveOrdered extends Ordering[String] {
  def compare(x: String, y: String): Int =
    x.compareToIgnoreCase(y)
}
