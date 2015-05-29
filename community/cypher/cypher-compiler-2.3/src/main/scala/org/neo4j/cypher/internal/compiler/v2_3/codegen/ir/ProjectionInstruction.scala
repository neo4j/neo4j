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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{MethodStructure, Namer}

// TODO: these objects are not really Instruction - should not extend it!
trait ProjectionInstruction extends Instruction {
  def init[E](generator: MethodStructure[E]): Unit

  def generateExpression[E](structure: MethodStructure[E]): E
}

object ProjectionInstruction {

  def literal(value: Long): ProjectionInstruction = ProjectLiteral(java.lang.Long.valueOf(value))

  def literal(value: Double): ProjectionInstruction = ProjectLiteral(java.lang.Double.valueOf(value))

  def literal(value: String): ProjectionInstruction = ProjectLiteral(value)

  def parameter(key: String): ProjectionInstruction = ProjectParameter(key)

  def add(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectAddition(lhs, rhs)

  def sub(lhs: ProjectionInstruction, rhs: ProjectionInstruction): ProjectionInstruction = ProjectSubtraction(lhs, rhs)
}

case class ProjectNodeProperty(id: String, token: Option[Int], propName: String, nodeIdVar: String, namer: Namer)
  extends ProjectionInstruction {

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
      body.incrementRows()
      body.incrementDbHits()
    }
    structure.load(localName)
  }

  override protected def operatorId = Some(id)

  override protected def children = Seq.empty
}

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

  override protected def operatorId = Some(id)

  override protected def children = Seq.empty
}

case class ProjectParameter(key: String) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = generator.expectParameter(key)

  override def generateExpression[E](structure: MethodStructure[E]): E = structure.parameter(key)

  override protected def children = Seq.empty
}

case class ProjectLiteral(value:Object) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = {}

  override def generateExpression[E](structure: MethodStructure[E]) = structure.constant(value)

  override protected def children = Seq.empty
}

case class ProjectNode(nodeIdVar: String) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = {}

  override def generateExpression[E](structure: MethodStructure[E]) = structure.materializeNode(nodeIdVar)

  override protected def children = Seq.empty
}

case class ProjectRelationship(relId: String) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = {}

  override def generateExpression[E](structure: MethodStructure[E]) = structure.materializeRelationship(relId)


  override protected def children = Seq.empty
}

case class ProjectAddition(lhs: ProjectionInstruction, rhs: ProjectionInstruction) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E]) =
    structure.add(lhs.generateExpression(structure), rhs.generateExpression(structure))

  override def children = Seq(lhs, rhs)
}

case class ProjectSubtraction(lhs: ProjectionInstruction, rhs: ProjectionInstruction) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E]) =
    structure.sub(lhs.generateExpression(structure), rhs.generateExpression(structure))


  override def children = Seq(lhs, rhs)
}

case class ProjectCollection(instructions: Seq[ProjectionInstruction]) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = instructions.foreach { instruction =>
    instruction.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E]) = structure.asList(instructions.map(_.generateExpression(structure)))

  override def children: Seq[Instruction] = instructions

}

case class ProjectMap(instructions: Map[String, ProjectionInstruction]) extends ProjectionInstruction {

  override def init[E](generator: MethodStructure[E]) = instructions.values.foreach { instruction =>
    instruction.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E]) =
    structure.asMap(instructions.mapValues(_.generateExpression(structure)))

  override def children: Seq[Instruction] = instructions.values.toSeq
}

case class Project(projections: Seq[ProjectionInstruction], parent: Instruction) extends Instruction {

  override def init[E](generator: MethodStructure[E]) = {
    projections.foreach { instruction =>
      instruction.init(generator)
    }
    parent.init(generator)
  }

  override def body[E](generator: MethodStructure[E]) = parent.body(generator)

  override protected def children = projections :+ parent
}
