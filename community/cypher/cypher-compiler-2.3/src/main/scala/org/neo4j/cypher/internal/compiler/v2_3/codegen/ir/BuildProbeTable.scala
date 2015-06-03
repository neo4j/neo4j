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

import org.neo4j.cypher.internal.compiler.v2_3.codegen._
import org.neo4j.cypher.internal.compiler.v2_3.symbols._

sealed trait BuildProbeTable extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.allocateProbeTable(name, tableType)

  protected val name: String

  def joinData: JoinData
  def tableType: JoinTableType

  override protected def children = Seq.empty
}

object BuildProbeTable {

  def apply(id: String, name: String, node: String, valueSymbols: Map[String, String])(implicit context: CodeGenContext): BuildProbeTable = {
    if (valueSymbols.isEmpty) BuildCountingProbeTable(id, name, node)
    else BuildRecordingProbeTable(id, name, node, valueSymbols)
  }
}

case class BuildRecordingProbeTable(id:String, name: String, node: String, valueSymbols: Map[String, String])
                                   (implicit context: CodeGenContext)
  extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E])(implicit ignored: CodeGenContext): Unit = {
    val value = generator.newTableValue(context.namer.newVarName(), valueTypeStructure)
    valueSymbols.foreach {
      case (fieldName, localName) => generator.putField(valueTypeStructure, value, CTNode, fieldName, localName)
    }
    generator.updateProbeTable(valueTypeStructure, name, node, value)
  }

  override protected def operatorId = Some(id)

  private val valueTypeField2VarName = valueSymbols.map {
    case (fieldName, symbol) => fieldName -> context.namer.newVarName()
  }

  private val valueTypeStructure: Map[String, CypherType] = valueSymbols.mapValues {
    case _ => CTNode
  }

  private val varName2ValueTypeField = valueTypeField2VarName.map {
    case (fieldName, localName) => localName -> fieldName
  }

  override val tableType = LongToListTable(valueTypeStructure,varName2ValueTypeField)

  val joinData: JoinData = JoinData(valueTypeField2VarName, name, tableType, id)

}

case class BuildCountingProbeTable(id: String, name: String, node: String) extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.updateProbeTableCount(name, node)

  override protected def operatorId = Some(id)

  override val tableType = LongToCountTable

  override def joinData = {
    JoinData(Map.empty, name, tableType, id)
  }
}

case class JoinData(vars: Map[String, String], tableVar: String, tableType: JoinTableType, id: String)
