/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{CreateUniqueAction, NamedExpectation, UniqueLink}
import org.neo4j.cypher.internal.frontend.v2_3.PatternException

case class CreateUniqueAst(patterns: Seq[AbstractPattern]) {
  def nextStep(): (Seq[CreateUniqueStartItem], Seq[NamedPath]) = {
    val results = patterns.map(translate)
    val (reducedLinks, reducedPaths) = results.reduce((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    (Seq(CreateUniqueStartItem(CreateUniqueAction(reducedLinks: _*))), reducedPaths)

  }

  private def translate(in: AbstractPattern): (Seq[UniqueLink], Seq[NamedPath]) = in match {
    case ParsedRelation(name, props,
    ParsedEntity(leftName, leftExp, leftProps, leftLabels),
    ParsedEntity(rightName, rightExp, rightProps, rightLabels), typ, dir, map) if typ.size == 1 =>
      val link = UniqueLink(
        left = NamedExpectation(leftName, leftExp, leftProps, leftLabels),
        right = NamedExpectation(rightName, rightExp, rightProps, rightLabels),
        rel = NamedExpectation(name, props),
        relType = typ.head,
        dir = dir
      )


      (Seq(link), Seq.empty)

    case ParsedNamedPath(name, patternsInNamedPath) =>
      val links: Seq[UniqueLink] = patternsInNamedPath.flatMap(p => translate(p)._1)

      (links, Seq(NamedPath(name, patternsInNamedPath:_*)))

    case _ => throw new PatternException("This pattern is not supported for CREATE UNIQUE")
  }
}
