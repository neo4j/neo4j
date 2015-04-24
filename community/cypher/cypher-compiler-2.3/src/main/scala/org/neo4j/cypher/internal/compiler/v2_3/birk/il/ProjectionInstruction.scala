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

import org.neo4j.cypher.internal.compiler.v2_3.birk.CodeGenerator.JavaTypes.{DOUBLE, LONG, NUMBER, OBJECT, OBJECTARRAY, STRING}
import org.neo4j.cypher.internal.compiler.v2_3.birk.codegen.Namer
import org.neo4j.cypher.internal.compiler.v2_3.birk.{CodeGenerator, JavaSymbol}

sealed trait ProjectionInstruction extends Instruction {
  def projectedVariable: JavaSymbol
  def generateCode() = ""
}

object ProjectionInstruction {
  def literal(value: Long): ProjectionInstruction = ProjectLiteral(JavaSymbol(value.toString + "L", LONG))
  def literal(value: Double): ProjectionInstruction = ProjectLiteral(JavaSymbol(value.toString, DOUBLE))
  def literal(value: String): ProjectionInstruction = ProjectLiteral(JavaSymbol( s""""$value"""", STRING))
  def literal(value: Boolean): ProjectionInstruction = ???
  def parameter(key: String): ProjectionInstruction = ProjectParameter(key)

  def add(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectAddition(lhs, rhs)
  def sub(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectSubtraction(lhs, rhs)
}

case class ProjectNodeProperty(token: Option[Int], propName: String, nodeIdVar: String, namer: Namer) extends ProjectionInstruction {
  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())

  def generateInit() = if (token.isEmpty)
      s"""if ( $propKeyVar == -1 )
         |{
         |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
         |}
       """.stripMargin
      else ""

  override def _importedClasses() =
    Set("org.neo4j.kernel.api.properties.Property")

  override def fields() = if (token.isEmpty) s"private int $propKeyVar = -1;" else ""

  def projectedVariable = JavaSymbol(s"ro.nodeGetProperty( $nodeIdVar, $propKeyVar ).value( null )", OBJECT)
}

case class ProjectRelProperty(token: Option[Int], propName: String, relIdVar: String, namer: Namer) extends ProjectionInstruction {
  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())

  def generateInit() =
    if (token.isEmpty)
      s"""if ( $propKeyVar == -1 )
         |{
         |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
         |}""".stripMargin
    else ""

  override def _importedClasses() =
    Set("org.neo4j.kernel.api.properties.Property")

  override def fields() = if (token.isEmpty) s"private int $propKeyVar = -1;" else ""

  def projectedVariable = JavaSymbol(s"ro.relationshipGetProperty( $relIdVar, $propKeyVar ).value( null )", OBJECT)
}

case class ProjectParameter(key: String) extends ProjectionInstruction {

  def generateInit() = ""


  def projectedVariable = JavaSymbol(s"""params.get( "$key" )""", OBJECT)

  def fields() = ""
}

case class ProjectLiteral(projectedVariable: JavaSymbol) extends ProjectionInstruction {

  def generateInit() = ""

  def fields() = ""
}

case class ProjectNodeOrRelationship(projectedVariable: JavaSymbol) extends ProjectionInstruction {

  def generateInit() = ""

  def fields() = ""
}

case class ProjectAddition(lhs: ProjectionInstruction, rhs: ProjectionInstruction) extends ProjectionInstruction {

  def projectedVariable: JavaSymbol = {
    val leftTerm = lhs.projectedVariable
    val rightTerm = rhs.projectedVariable
    (leftTerm.javaType, rightTerm.javaType) match {
      case (LONG, LONG) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", LONG)
      case (LONG, DOUBLE) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", DOUBLE)
      case (LONG, STRING) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)

      case (DOUBLE, DOUBLE) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", DOUBLE)
      case (DOUBLE, LONG) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", DOUBLE)
      case (DOUBLE, STRING) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)

      case (STRING, STRING) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)
      case (STRING, LONG) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)
      case (STRING, DOUBLE) => JavaSymbol(s"${leftTerm.name} + ${rightTerm.name}", STRING)

      case (_, _) => JavaSymbol(s"CompiledMathHelper.add( ${leftTerm.name}, ${rightTerm.name} )", OBJECT)
    }
  }

  def generateInit() = ""

  def fields() = ""

  override def _importedClasses(): Set[String] = Set("org.neo4j.cypher.internal.compiler.v2_3.birk.CompiledMathHelper")
}

case class ProjectSubtraction(lhs: ProjectionInstruction, rhs: ProjectionInstruction) extends ProjectionInstruction {

  def projectedVariable: JavaSymbol = {
    val leftTerm = lhs.projectedVariable
    val rightTerm = rhs.projectedVariable
    (leftTerm.javaType, rightTerm.javaType) match {
      case (LONG, LONG) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", LONG)
      case (LONG, DOUBLE) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", DOUBLE)

      case (DOUBLE, DOUBLE) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", DOUBLE)
      case (DOUBLE, LONG) => JavaSymbol(s"${leftTerm.name} - ${rightTerm.name}", DOUBLE)

      case (_, _) => JavaSymbol(s"CompiledMathHelper.subtract( ${leftTerm.name}, ${rightTerm.name} )", NUMBER)
    }
  }

  def generateInit() = ""

  def fields() = ""

  override def _importedClasses(): Set[String] = Set("org.neo4j.cypher.internal.compiler.v2_3.birk.CompiledMathHelper")
}

case class ProjectCollection(instructions: Seq[ProjectionInstruction]) extends ProjectionInstruction {

  override def children: Seq[Instruction] = instructions

  def generateInit() = ""

  def projectedVariable = JavaSymbol(instructions.map(_.projectedVariable.name).mkString("new Object[]{", ",", "}"), OBJECTARRAY)

  def fields() = ""
}

case class ProjectProperties(projections:Seq[ProjectionInstruction], parent:Instruction) extends Instruction {
  override def generateCode()= generate(_.generateCode())

  override protected def children: Seq[Instruction] = projections :+ parent

  override def fields() = generate(_.fields())

  override def generateInit() = generate(_.generateInit())

  private def generate(f : Instruction => String) = projections.map(f(_)).mkString("", CodeGenerator.n, CodeGenerator.n) + f(parent)
}
