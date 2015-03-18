/**
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.graphdb.Direction

case class ExpandC(fromVar: String, relVar: String, dir: Direction, types: Map[String, String], toVar: String, inner: Instruction) extends LoopDataGenerator {
  private val visitMethod =
    if (dir == Direction.OUTGOING)
      s"""@Override
         |public void visit( long relId, int type, long startNode, long $toVar ) throws KernelException
         |{
         |${inner.generateCode()}
         |}
       """.stripMargin
    else if (dir == Direction.INCOMING)
      s"""@Override
         |public void visit( long relId, int type, long $toVar, long endNode ) throws KernelException
         |{
         |${inner.generateCode()}
         |}
       """.stripMargin
    else {
      s"""@Override
         |public void visit( long relId, int type, long startNode, long endNode ) throws KernelException
         |{
         |long $toVar;
         |if ( $fromVar == startNode )
         |{
         |$toVar = endNode;
         |}
         |else
         |{
         |$toVar = startNode;
         |}
         |${inner.generateCode()}
         |}
       """.stripMargin
    }

  def generateCode() =
    if (types.isEmpty)
      s"ro.nodeGetRelationships( $fromVar, Direction.$dir )"
    else
      s"ro.nodeGetRelationships( $fromVar, Direction.$dir, ${types.map(_._1).mkString(",")} )"

  def generateVariablesAndAssignment() =
    s"""ro.relationshipVisit( $relVar, new RelationshipVisitor<KernelException>()
       |{
       |$visitMethod
       |});""".stripMargin

  override def _importedClasses() =  Set(
    "org.neo4j.graphdb.Direction",
    "org.neo4j.collection.primitive.PrimitiveLongIterator",
    "org.neo4j.kernel.api.exceptions.KernelException",
    "org.neo4j.kernel.impl.api.RelationshipVisitor")

  def javaType = "org.neo4j.kernel.impl.api.store.RelationshipIterator"

  //TODO we should only add this when name is not resolved, otherwise inline it
  def generateInit() =
    s"""${types.map(s =>
      s"""if ( ${s._1} == -1 )
         |{
         |${s._1} = ro.relationshipTypeGetForName( "${s._2}" );
         |}
       """.stripMargin).mkString("\n")}
       |${inner.generateInit()}""".stripMargin

  override def fields() =
    s"""${types.map(s => s"int ${s._1} = -1;").mkString("\n")}
       |${inner.fields()}
     """.stripMargin

  override def children = Seq(inner)

}
