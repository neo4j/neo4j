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
package org.neo4j.cypher.internal.frontend.v3_3

sealed trait SemanticErrorDef {
  def msg: String
  def position: InputPosition
  def references: Seq[InputPosition]
}

final case class SemanticError(msg: String, position: InputPosition, references: InputPosition*) extends SemanticErrorDef

sealed trait UnsupportedOpenCypher extends SemanticErrorDef

final case class ClauseError(clause: String, position: InputPosition) extends UnsupportedOpenCypher {

  override val msg: String = s"The referenced clause $clause is not supported by Neo4j"
  override def references = Seq.empty
}

final case class PatternError(msg: String, position: InputPosition) extends UnsupportedOpenCypher {
  override def references = Seq.empty
}
