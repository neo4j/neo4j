/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.SymbolicName

object PrettyStringCreator {
  def raw(s: String): PrettyString = PrettyString(s)

  def apply(s: SymbolicName): PrettyString = PrettyString(PlanDescriptionArgumentSerializer.asPrettyString(s))

  def apply(n: Namespace): PrettyString = PrettyString(PlanDescriptionArgumentSerializer.asPrettyString(n))

  def apply(expr: Expression): PrettyString = PrettyString(PlanDescriptionArgumentSerializer.asPrettyString(expr))

  // TODO should we simply add removeGeneratedNames here?
  def apply(variableName: String): PrettyString = PrettyString(ExpressionStringifier.backtick(variableName))

  implicit class PrettyStringInterpolator(val sc: StringContext) extends AnyVal {
    def pretty(args: PrettyString*): PrettyString = {
      val connectors = sc.parts.iterator
      val expressions = args.iterator
      val buf = new StringBuffer(connectors.next)
      while(connectors.hasNext) {
        buf append expressions.next.prettifiedString
        buf append connectors.next
      }
      PrettyString(buf.toString)
    }
  }

  implicit class PrettyStringMaker(traversableOnce: TraversableOnce[PrettyString]) {
    def mkPrettyString(sep: String): PrettyString = PrettyString(traversableOnce.mkString(sep))
    def mkPrettyString: PrettyString = PrettyString(traversableOnce.mkString)
    def mkPrettyString(start: String, sep: String, end: String): PrettyString = PrettyString(traversableOnce.mkString(start, sep, end))
  }
}
