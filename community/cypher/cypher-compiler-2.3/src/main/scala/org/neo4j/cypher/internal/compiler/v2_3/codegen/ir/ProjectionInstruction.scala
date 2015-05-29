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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.JavaTypes.{DOUBLE, LIST, LONG, MAP, NUMBER, OBJECT, STRING}
import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.{JavaString, JavaSymbol}
import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenerator, KernelExceptionCodeGen, MethodStructure, Namer}

// TODO: these objects are not really Instruction - should not extend it!
trait ProjectionInstruction extends Instruction {
  def generateExpression[E](structure: MethodStructure[E]): E

  def projectedVariable: JavaSymbol

  def generateCode() = ""
}

object ProjectionInstruction {

  def literal(value: Long): ProjectionInstruction = ProjectLiteral(JavaSymbol(value.toString + "L", LONG), java.lang.Long.valueOf(value))

  def literal(value: Double): ProjectionInstruction = ProjectLiteral(JavaSymbol(value.toString, DOUBLE), java.lang.Double.valueOf(value))

  def literal(value: String): ProjectionInstruction = ProjectLiteral(JavaSymbol(s""""$value"""", STRING), value)

  def parameter(key: String): ProjectionInstruction = ProjectParameter(key)

  def add(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectAddition(lhs, rhs)

  def sub(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectSubtraction(lhs, rhs)
}

case class ProjectNodeProperty(id: String, token: Option[Int], propName: String, nodeIdVar: String, namer: Namer)
  extends ProjectionInstruction {

  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())
  private val methodName = namer.newMethodName()

  override def init[E](generator: MethodStructure[E]) = if (token.isEmpty) generator.lookupPropertyKey(propName, propKeyVar)

  override def generateExpression[E](structure: MethodStructure[E]): E = {
    val localName = namer.newVarName()
    structure.declareProperty(localName)
    structure.trace(id) { body =>
      if (token.isEmpty)
        body.nodeGetPropertyForVar(nodeIdVar, propKeyVar, localName)
      else
        body.nodeGetPropertyById(nodeIdVar, token.get, localName)
      body.incrementRows()
      body.incrementDbHits()
    }
    structure.load(localName)
  }

  def generateInit() = if (token.isEmpty)
    s"""if ( $propKeyVar == -1 )
        |{
        |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
        |}
       """.stripMargin
  else ""

  override def members() = if (token.isEmpty) s"private int $propKeyVar = -1;" else ""

  def projectedVariable = JavaSymbol(s"$methodName( $nodeIdVar )", OBJECT)

  override protected def importedClasses =
    Set("org.neo4j.kernel.api.properties.Property", "org.neo4j.kernel.api.exceptions.EntityNotFoundException")

  override protected def exceptions = Set(KernelExceptionCodeGen)

  override protected def operatorId = Some(id)

  override protected def method = Some(new Method() {
    override def name: String = methodName

    override def generateCode: String = {
      val eventVar = "event_" + id
      s"""
         |private Object $methodName( long nodeId ) throws EntityNotFoundException
         |{
         |try ( QueryExecutionEvent $eventVar = tracer.executeOperator( $id ) )
         |{
         |$eventVar.dbHit();
         |$eventVar.row();
         |return ro.nodeGetProperty( nodeId, $propKeyVar ).value( null );
         |}
         |}
      """.stripMargin
    }
  })

  override protected def children = Seq.empty

}

//TODO this should do tracing
case class ProjectRelProperty(id:String, token: Option[Int], propName: String, relIdVar: String, namer: Namer)
  extends ProjectionInstruction {

  private val propKeyVar = token.map(_.toString).getOrElse(namer.newVarName())

  override def init[E](generator: MethodStructure[E]) = if (token.isEmpty) generator.lookupPropertyKey(propName, propKeyVar)

  override def generateExpression[E](structure: MethodStructure[E]): E = {
    val localName = namer.newVarName()
    structure.declareProperty(localName)
    structure.trace(id) { body =>
      if (token.isEmpty)
        body.relationshipGetPropertyForVar(relIdVar, propKeyVar, localName)
      else
        body.relationshipGetPropertyById(relIdVar, token.get, localName)
      body.incrementRows()
      body.incrementDbHits()
    }
    structure.load(localName)
  }

  def generateInit() =
    if (token.isEmpty)
      s"""if ( $propKeyVar == -1 )
          |{
          |$propKeyVar = ro.propertyKeyGetForName( "$propName" );
          |}""".stripMargin
    else ""

  override def members() = if (token.isEmpty) s"private int $propKeyVar = -1;" else ""

  def projectedVariable = JavaSymbol(s"ro.relationshipGetProperty( $relIdVar, $propKeyVar ).value( null )", OBJECT)

  override protected def importedClasses = Set("org.neo4j.kernel.api.properties.Property")

  override protected def exceptions = Set(KernelExceptionCodeGen)

  override protected def operatorId = Some(id)

  override protected def children = Seq.empty
}

case class ProjectParameter(key: String) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = generator.expectParameter(key)

  override def generateExpression[E](structure: MethodStructure[E]): E = structure.parameter(key)

  def generateInit() =
    s"""if( !params.containsKey( "${key.toJava}" ) )
       |{
       |throw new ParameterNotFoundException( "Expected a parameter named ${key.toJava}" );
       |}
    """.stripMargin

  def projectedVariable = JavaSymbol(s"""params.get( "${key.toJava}" )""", OBJECT)

  def members() = ""

  override protected def importedClasses = Set("org.neo4j.cypher.internal.compiler.v2_3.ParameterNotFoundException")

  override protected def children = Seq.empty
}

case class ProjectLiteral(projectedVariable: JavaSymbol, value:Object) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) = structure.constant(value)

  def generateInit() = ""

  def members() = ""

  override protected def children = Seq.empty
}

case class ProjectNode(nodeIdVar: JavaSymbol) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) = structure.materializeNode(nodeIdVar.name)

  def projectedVariable = nodeIdVar.withProjectedSymbol(CodeGenerator.getNodeById(nodeIdVar.name))

  def generateInit() = ""

  def members() = ""

  override protected def children = Seq.empty
}

case class ProjectRelationship(relId: JavaSymbol) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) = structure.materializeRelationship(relId.name)


  def projectedVariable = relId.withProjectedSymbol(CodeGenerator.getRelationshipById(relId.name))

  def generateInit() = ""

  def members() = ""

  override protected def children = Seq.empty
}

case class ProjectAddition(lhs: ProjectionInstruction, rhs: ProjectionInstruction) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) = {
    structure.add(
      lhs.projectedVariable.javaType, lhs.generateExpression(structure),
      rhs.projectedVariable.javaType, rhs.generateExpression(structure))
  }

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

  def members() = ""

  override def children = Seq(lhs, rhs)

  override protected def importedClasses = Set("org.neo4j.cypher.internal.compiler.v2_3.codegen.CompiledMathHelper")
}

case class ProjectSubtraction(lhs: ProjectionInstruction, rhs: ProjectionInstruction) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) = {
    structure.sub(
      lhs.projectedVariable.javaType, lhs.generateExpression(structure),
      rhs.projectedVariable.javaType, rhs.generateExpression(structure))
  }

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

  def members() = ""

  override def children = Seq(lhs, rhs)

  override protected def importedClasses = Set("org.neo4j.cypher.internal.compiler.v2_3.codegen.CompiledMathHelper")
}

case class ProjectCollection(instructions: Seq[ProjectionInstruction]) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) = structure.asList(instructions.map(_.generateExpression(structure)))

  override def children: Seq[Instruction] = instructions

  def generateInit() = ""

  def projectedVariable = asJavaList(_.projectedVariable.name)
    .withProjectedSymbol(asJavaList(_.projectedVariable.materialize.name))

  def members() = ""

  private def asJavaList(f: ProjectionInstruction => String) =
    JavaSymbol(instructions.map(f).mkString("Arrays.asList(", ",", ")"), LIST)

  override protected def importedClasses = Set("java.util.Arrays")
}

case class ProjectMap(instructions: Map[String, ProjectionInstruction]) extends ProjectionInstruction {

  override def generateExpression[E](structure: MethodStructure[E]) =
    structure.asMap(instructions.mapValues(_.generateExpression(structure)))

  override def children: Seq[Instruction] = instructions.values.toSeq

  def generateInit() = ""

  def projectedVariable = asJavaMap(_.projectedVariable.name)
    .withProjectedSymbol(asJavaMap(_.projectedVariable.materialize.name))

  def members() = ""

  private def asJavaMap(f: ProjectionInstruction => String) = JavaSymbol(
    instructions.map {
      case (key, value) => s""""$key", ${f(value)}"""
    }.mkString("org.neo4j.helpers.collection.MapUtil.map(", ",", ")"), MAP)
}

case class Project(projections: Seq[ProjectionInstruction], parent: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E]) = parent.body(generator)

  override def generateCode() = generate(_.generateCode())

  override protected def children = projections :+ parent

  override def members() = generate(_.members())

  override def generateInit() = generate(_.generateInit())

  private def generate(f: Instruction => String) = projections.map(f(_))
    .mkString("", CodeGenerator.n, CodeGenerator.n) + f(parent)
}
