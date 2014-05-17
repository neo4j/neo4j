/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

case object queryGraphDocGenerator extends NestedDocGenerator[Any] {

  import Doc._

  protected val instance: RecursiveDocGenerator[Any] = {
    case qg: QueryGraph => (inner: DocGenerator[Any]) =>
      val args = section("GIVEN", starList(qg.argumentIds.toList)(inner))
      val patterns = section("MATCH", sepList(
        qg.patternNodes.map(id => cons(text("("), cons(inner(id), cons(")")))).toList ++
        qg.patternRelationships.map(inner).toList
      ))

      val optionalMatches = qg.optionalMatches.map(inner).toList
      val optional =
        if (optionalMatches.isEmpty) nil
        else section("OPTIONAL", scalaGroup("", open="{ ", close=" }")(optionalMatches))

      val where = section("WHERE", inner(qg.selections))

      group(breakList(List(args, patterns, optional, where).filter(_ != NilDoc)))
  }

  private def section(start: String, inner: Doc): Doc = inner match {
    case NilDoc => nil
    case _      => group(breakCons(text(start), nest(inner)))
  }

  private def starList[T](list: List[T])(inner: DocGenerator[Any]) =
    if (list.isEmpty)
      text("*")
    else
      sepList(list.map(inner))
}
