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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.perty.{CustomDocGen, Doc, DocConverter, mkDocDrill}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Predicate, Selections}

case object plannerParticleDocGen extends CustomDocGen[Any] {

  import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._

  def newDocDrill = mkDocDrill[Any]() {
    case idName: IdName => inner => idName.asDoc
    case predicate: Predicate => inner => predicate.asDoc(inner)
    case selections: Selections => inner => selections.asDoc(inner)
  }

  implicit class idNameConverter(idName: IdName) {
    def asDoc = text(idName.name)
  }

  implicit class predicateConverter(predicate: Predicate) {
    def asDoc(pretty: DocConverter[Any]) = {
      val pred = sepList(predicate.dependencies.map(pretty), break = breakSilent)
      val predBlock = block("Predicate", open = "[", close = "]")(pred)
      group("Predicate" :: brackets(pred, break = noBreak) :: parens(pretty(predicate.expr)))
    }
  }

  implicit class selectionsConverter(selections: Selections) {
    def asDoc(pretty: DocConverter[Any]) =
      sepList(selections.predicates.map(pretty))
  }
}
