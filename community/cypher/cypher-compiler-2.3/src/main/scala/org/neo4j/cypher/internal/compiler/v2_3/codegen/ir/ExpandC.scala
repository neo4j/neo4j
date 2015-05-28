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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{MethodStructure, KernelExceptionCodeGen}
import org.neo4j.graphdb.Direction

case class ExpandC(id: String, fromVar: String, relVar: String, dir: Direction,
                   types: Map[String, String], toVar: String, inner: Instruction)
  extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E]) = {
    types.foreach {
      case (typeVar,relType) => generator.lookupRelationshipTypeId(typeVar, relType)
    }
    inner.init(generator)
  }

  override def produceIterator[E](iterVar: String, generator: MethodStructure[E]) = {
    if(types.isEmpty)
      generator.nodeGetAllRelationships(iterVar, fromVar, dir)
    else
      generator.nodeGetRelationships(iterVar, fromVar, dir, types.keys.toSeq)
    generator.incrementDbHits()
  }

  override def produceNext[E](nextVar: String, iterVar: String, generator: MethodStructure[E]) =
    generator.nextRelationshipNode(toVar, iterVar, dir, fromVar, relVar)

  override def children = Seq(inner)
}
