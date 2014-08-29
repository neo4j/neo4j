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
package org.neo4j.cypher.internal.compiler.v2_2.docbuilders

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.simpleDocBuilder
import org.neo4j.cypher.internal.compiler.v2_2.perty._

import scala.util.Try

case object astParticleDocBuilder extends CustomDocBuilder[Any] {
  def newDocGenerator = DocGenerator {
    case particle: ASTParticle =>
      // use get() to find where we lack pretty printing support
      inner => astParticleConverter(inner)(particle).getOrElse(simpleDocBuilder.docGenerator.applyWithInner(inner)(particle))
  }
}

case class astParticleConverter(pretty: FixedDocGenerator[Any]) extends (ASTParticle => Option[Doc]) {

  def apply(particle: ASTParticle) = Try[Doc] {
    particle match {
      case labelName: LabelName => labelName.asDoc
      case symbolicName: SymbolicName => symbolicName.asDoc
    }
  }.toOption

  implicit class SymbolicNameConverter(symbolicName: SymbolicName) {
    def asDoc = symbolicName.name
  }

  implicit class LabelNameConverter(labelName: LabelName) {
    def asDoc = group(":" :: labelName.name)
  }
}
