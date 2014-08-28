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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
import symbols._

sealed trait Hint extends ASTNode with SemanticCheckable {
  def identifier: Identifier
}

case class UsingIndexHint(identifier: Identifier, label: LabelName, property: Identifier)(val position: InputPosition) extends Hint {
  def semanticCheck = identifier.ensureDefined chain identifier.expectType(CTNode.covariant)

  override def toDoc = group("USING" :/: "INDEX" :/: group(identifier :: block(label)(property)))
}

case class UsingScanHint(identifier: Identifier, label: LabelName)(val position: InputPosition) extends Hint {
  def semanticCheck = identifier.ensureDefined chain identifier.expectType(CTNode.covariant)

  override def toDoc = group("USING" :/: "SCAN" :/: group(identifier :: label))
}
