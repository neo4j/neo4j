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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.CodeGenerator.n
import org.neo4j.cypher.internal.compiler.v2_3.codegen.JavaUtils.JavaSymbol
import org.neo4j.cypher.internal.compiler.v2_3.codegen._
import org.neo4j.cypher.internal.compiler.v2_3.symbols._

sealed trait BuildProbeTable extends Instruction {

  override def init[E](generator: MethodStructure[E]) = generator.allocateProbeTable(name, tableType)

  def producedType: String

  def generateFetchCode: CodeThunk

  protected val name:String
  val tableType: JoinTableType
}

object BuildProbeTable {

  def apply(id: String, name: String, node: String, valueSymbols: Map[String, JavaSymbol],
            namer: Namer): BuildProbeTable = {
    if (valueSymbols.isEmpty) BuildCountingProbeTable(id, name, node, namer)
    else BuildRecordingProbeTable(id, name, node, valueSymbols, namer)
  }
}

case class BuildRecordingProbeTable(id:String, name: String, node: String, valueSymbols: Map[String, JavaSymbol], namer: Namer)
  extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E]): Unit = {
    val value = generator.newTableValue(namer.newVarName(), valueTypeStructure)
    valueSymbols.foreach {
      case (fieldName, JavaSymbol(localName, "long", _, _, _)) => generator.putField(valueTypeStructure, value, CTNode, fieldName, localName)
    }
    generator.updateProbeTable(valueTypeStructure, name, node, value)
  }

  private val valueType = s"ValueTypeIn$name"

  private val innerClassDeclaration =
    s"""static class $valueType
        |{
        |${valueSymbols.map { case (k, v) => s"${v.javaType} $k;" }.mkString(n)}
        |}""".stripMargin

  def members() = innerClassDeclaration

  def generateInit() = s"final $producedType $name = Primitive.longObjectMap();"

  def generateCode() = {
    val listName = namer.newVarName()
    val elemName = namer.newVarName()
    s"""$valueType $elemName  = new $valueType();
       |${valueSymbols.map(s => s"$elemName.${s._1} = ${s._2.name};").mkString(n)}
       |ArrayList<$valueType> $listName = $name.get( $node );
       |if ( null == $listName )
       |{
       |$name.put( $node, ($listName = new ArrayList<>( ) ) );
       |}
       |$listName.add( $elemName );""".stripMargin
  }

  private val valueTypeField2VarName = valueSymbols.map {
    case (fieldName, symbol) => fieldName -> namer.newVarName(symbol.javaType)
  }

  private val valueTypeStructure: Map[String, CypherType] = valueSymbols.mapValues {
    case JavaSymbol(_, "long", _, _, _) => CTNode
  }

  private val varName2ValueTypeField = valueTypeField2VarName.map {
    case (fieldName, JavaSymbol(localName, "long", _, _, _)) => localName -> fieldName
  }

  override val tableType = LongToListTable(valueTypeStructure,varName2ValueTypeField)

  val generateFetchCode: CodeThunk = {
    val listName = namer.newVarName()
    val elemName = namer.newVarName()
    val code = (key: String, action: Instruction) => {
      s"""ArrayList<$valueType> $listName = $name.get( $key);
          |if ( $listName!= null )
          |{
          |for ($valueType $elemName : $listName )
          |{
          |${valueTypeField2VarName.map { case (fieldName, symbol) => s"final ${symbol.javaType} ${symbol.name} = $elemName.$fieldName;"}.mkString(n)}
          |${action.generateCode()}
          |}
          |}""".stripMargin
    }
    CodeThunk(valueTypeField2VarName, code, name, tableType, id)
  }

  override protected def importedClasses = Set(
    "org.neo4j.collection.primitive.PrimitiveLongObjectMap",
    "java.util.ArrayList"
  )

  def producedType: String = s"PrimitiveLongObjectMap<ArrayList<$valueType>>"

  override protected def children = Seq.empty
}

case class BuildCountingProbeTable(id: String, name: String, node: String, namer: Namer) extends BuildProbeTable {

  override def body[E](generator: MethodStructure[E]) = generator.updateProbeTableCount(name, node)

  def generateInit() = s"final PrimitiveLongIntMap $name = Primitive.longIntMap();"

  def generateCode() =
    s"""int count = $name.get( $node );
       |if ( count == LongKeyIntValueTable.NULL )
       |{
       |$name.put( $node, 1 );
       |}
       |else
       |{
       |$name.put( $node, count + 1 );
       |}""".stripMargin

  override protected def importedClasses = Set(
    "org.neo4j.collection.primitive.PrimitiveLongIntMap",
    "org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable")

  override protected def operatorId = Some(id)

  def producedType: String = "PrimitiveLongIntMap"

  def members() = ""

  def valueType = "int"

  override val tableType = LongToCountTable

  override def generateFetchCode = {
    val timesSeen = namer.newVarName()
    val eventVar = s"event_$id"
    val code = (key: String, action: Instruction) => {
      s"""
         |try ( QueryExecutionEvent $eventVar = tracer.executeOperator( $id ) )
         |{
         |int $timesSeen = $name.get( $key );
         |if ( $timesSeen != LongKeyIntValueTable.NULL )
         |{
         |for ( int i = 0; i < $timesSeen; i++ )
         |{
         |$eventVar.row();
         |${action.generateCode()}
         |}
         |}
         |}"""
        .stripMargin
    }
    CodeThunk(Map.empty, code, name, tableType, id)
  }

  override protected def children = Seq.empty
}

case class CodeThunk(vars: Map[String, JavaSymbol], generator: (String, Instruction) => String, tableVar:String, tableType:JoinTableType, id:String) {

  def apply(key: String, instruction: Instruction) = generator(key, instruction)
}
