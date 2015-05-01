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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.cypher.internal.compiler.v2_3.birk.JavaSymbol
import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.JavaString

case class ScanForLabel(labelName: String, labelVar: JavaSymbol) extends Instruction with LoopDataGenerator {
  def generateCode() = s"""ro.nodesGetForLabel( ${labelVar.name} )"""

  def generateVariablesAndAssignment() = ""

  //TODO only generate this if label token not known at compile time
  def generateInit() =
    s"""if ( ${labelVar.name} == -1 )
       |{
       |${labelVar.name} = ro.labelGetForName( "${labelName.toJava}" );
       |}""".stripMargin

  override def fields() = s"private ${labelVar.javaType} ${labelVar.name} = -1;"
  override def _importedClasses() =
    Set("org.neo4j.collection.primitive.PrimitiveLongIterator")

  def javaType = "PrimitiveLongIterator"

}
