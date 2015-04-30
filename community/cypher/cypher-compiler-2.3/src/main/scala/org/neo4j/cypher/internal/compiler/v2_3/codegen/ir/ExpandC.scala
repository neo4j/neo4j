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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.KernelExceptionCodeGen
import org.neo4j.graphdb.Direction

case class ExpandC(id: String, fromVar: String, relVar: String, dir: Direction,
                   types: Map[String, String], toVar: String, inner: Instruction)
  extends LoopDataGenerator {

  private val theBody =
    if (dir == Direction.OUTGOING)
      s"""$toVar = rel.endNode();
       """.stripMargin
    else if (dir == Direction.INCOMING)
      s"""$toVar = rel.startNode();
       """.stripMargin
    else {
      s"""if ( $fromVar == rel.startNode() )
         |{
         |$toVar = rel.endNode();
         |}
         |else
         |{
         |$toVar = rel.startNode();
         |}
       """.stripMargin
    }

  def generateCode() =
    if (types.isEmpty)
      s"ro.nodeGetRelationships( $fromVar, Direction.$dir )"
    else
      s"ro.nodeGetRelationships( $fromVar, Direction.$dir, ${types.map(_._1).mkString(",")} )"

  def generateVariablesAndAssignment() =
    s"""long $toVar;
       |{
       |RelationshipDataExtractor rel = new RelationshipDataExtractor();
       |ro.relationshipVisit( $relVar, rel );
       |$theBody
       |}
       |${inner.generateCode()}""".stripMargin

  override def _importedClasses() = Set(
    "org.neo4j.graphdb.Direction",
    "org.neo4j.collection.primitive.PrimitiveLongIterator",
    "org.neo4j.kernel.impl.api.RelationshipDataExtractor")

  override def _exceptions() = Set(KernelExceptionCodeGen)

  def javaType = "org.neo4j.kernel.impl.api.store.RelationshipIterator"

  //TODO we should only add this when name is not resolved, otherwise inline it
  def generateInit() =
    s"""${types.map(s =>
      s"""if ( ${s._1} == -1 )
         |{
         |${s._1} = ro.relationshipTypeGetForName( "${s._2}" );
         |}""".stripMargin).mkString("\n")}
       |${inner.generateInit()}""".stripMargin

  override def members() =
    s"""${types.map(s => s"int ${s._1} = -1;").mkString("\n")}
       |${inner.members()}""".stripMargin

  override def children = Seq(inner)

}
