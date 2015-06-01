/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{MethodStructure, Namer}

case class NodeProperty(id: String, token: Option[Int], propName: String, nodeIdVar: String, namer: Namer)
  extends CodeGenExpression {

  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())

  override def init[E](generator: MethodStructure[E]) = if (token.isEmpty) generator.lookupPropertyKey(propName, propKeyVar)

  override def generateExpression[E](structure: MethodStructure[E]): E = {
    val localName = namer.newVarName()
    structure.declareProperty(localName)
    structure.trace(id) { body =>
      if (token.isEmpty)
        body.nodeGetPropertyForVar(nodeIdVar, propKeyVar, localName)
      else
        body.nodeGetPropertyById(nodeIdVar, token.get, localName)
      body.incrementDbHits()
    }
    structure.load(localName)
  }
}
