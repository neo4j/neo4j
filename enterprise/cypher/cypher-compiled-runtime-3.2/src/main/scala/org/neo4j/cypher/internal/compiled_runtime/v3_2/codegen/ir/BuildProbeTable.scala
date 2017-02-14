/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen._
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi._

sealed trait BuildProbeTable extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.allocateProbeTable(name, tableType)

  protected val name: String

  def joinData: JoinData
  def tableType: JoinTableType

  override protected def children = Seq.empty
}

object BuildProbeTable {

  def apply(id: String, name: String, nodes: Set[Variable], valueSymbols: Map[String, Variable])(implicit context: CodeGenContext): BuildProbeTable = {
    if (valueSymbols.isEmpty) BuildCountingProbeTable(id, name, nodes)
    else BuildRecordingProbeTable(id, name, nodes, valueSymbols)
  }
}

case class BuildRecordingProbeTable(id: String, name: String, nodes: Set[Variable], valueSymbols: Map[String, Variable])
                                   (implicit context: CodeGenContext)
  extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E])(implicit ignored: CodeGenContext): Unit = {
    generator.trace(id, Some(this.getClass.getSimpleName)) { body =>
      val tuple = body.newTableValue(context.namer.newVarName(), tupleDescriptor)
      fieldToVarName.foreach {
        case (fieldName, localName) => body.putField(tupleDescriptor, tuple, fieldName, localName.incoming.name)
      }
      body.updateProbeTable(tupleDescriptor, name, tableType, keyVars = nodes.toIndexedSeq.map(_.name), tuple)
    }
  }

  override protected def operatorId = Set(id)

  private val fieldToVarName = valueSymbols.map {
    case (variable, incoming) => (context.namer.newVarName(), VariableData(variable, incoming,  incoming.copy(name = context.namer.newVarName())))
  }

  private val varNameToField = fieldToVarName.map {
    case (fieldName, localName) => localName.outgoing.name -> fieldName
  }

  private val tupleDescriptor = SimpleTupleDescriptor(fieldToVarName.mapValues(c => c.outgoing.codeGenType))

  override val tableType = if (nodes.size == 1) LongToListTable(tupleDescriptor, varNameToField)
                           else LongsToListTable(tupleDescriptor, varNameToField)

  val joinData: JoinData = JoinData(fieldToVarName, name, tableType, id)
}

case class BuildCountingProbeTable(id: String, name: String, nodes: Set[Variable]) extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.trace(id, Some(this.getClass.getSimpleName)) { body =>
      body.updateProbeTableCount(name, tableType, nodes.toIndexedSeq.map(_.name))
    }

  override protected def operatorId = Set(id)

  override val tableType = if (nodes.size == 1) LongToCountTable else LongsToCountTable

  override def joinData = {
    JoinData(Map.empty, name, tableType, id)
  }
}
case class VariableData(variable: String, incoming: Variable, outgoing: Variable)
case class JoinData(vars: Map[String, VariableData], tableVar: String, tableType: JoinTableType, id: String)
