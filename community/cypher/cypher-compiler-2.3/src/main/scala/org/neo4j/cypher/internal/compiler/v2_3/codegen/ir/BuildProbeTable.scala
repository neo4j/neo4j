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
import org.neo4j.cypher.internal.compiler.v2_3.codegen.{Namer, CodeGenerator, JavaSymbol}

sealed trait BuildProbeTable extends Instruction {
  def producedType: String
  def generateFetchCode: CodeThunk

}
object BuildProbeTable {
  def apply(id:String, name: String, node: String, valueSymbols: Map[String, JavaSymbol], namer: Namer): BuildProbeTable = {
    if (valueSymbols.isEmpty) BuildCountingProbeTable(id, name, node, namer)
    else BuildRecordingProbeTable(name, node, valueSymbols, namer)
  }
}

case class BuildRecordingProbeTable(name: String, node: String, valueSymbols: Map[String, JavaSymbol], namer: Namer) extends BuildProbeTable {

  private val valueType = s"ValueTypeIn$name"

  private val innerClassDeclaration =
    s"""static class $valueType
        |{
        |${valueSymbols.map {case (k, v) => s"${v.javaType} $k;"}.mkString(n)}
        |}""".stripMargin

  def members() = innerClassDeclaration

  def generateInit() = s"final $producedType $name = Primitive.longObjectMap( );"

  def generateCode() = {
    val listName = namer.newVarName()
    val elemName = namer.newVarName()
    s"""ArrayList<$valueType> $listName = $name.get( $node );
       |if ( null == $listName )
       |{
       |$name.put( $node, ($listName = new ArrayList<>( ) ) );
       |}
       |$valueType $elemName  = new $valueType();
       |${valueSymbols.map(s => s"$elemName.${s._1} = ${s._2.name};").mkString(n)}
       |$listName.add( $elemName );""".stripMargin
  }

  val generateFetchCode: CodeThunk = {
    val symbols = valueSymbols.map {
      case (id, symbol) => id -> namer.newVarName(symbol.javaType)
    }

    val listName = namer.newVarName()
    val elemName = namer.newVarName()
    val code = (key: String, action: Instruction) => {
      s"""ArrayList<$valueType> $listName = $name.get( $key);
          |if ( $listName!= null )
          |{
          |for ($valueType $elemName : $listName )
          |{
          |${symbols.map { case (id, symbol) => s"final ${symbol.javaType} ${symbol.name} = $elemName.$id;"}.mkString(n)}
          |${action.generateCode()}
          |}
          |}""".stripMargin
      }
    CodeThunk(symbols, code)
  }

  override def _importedClasses() = Set(
    "org.neo4j.collection.primitive.PrimitiveLongObjectMap",
    "java.util.ArrayList"
  )

  def producedType: String = s"PrimitiveLongObjectMap<ArrayList<$valueType>>"

}

case class BuildCountingProbeTable(id: String, name: String, node: String, namer: Namer) extends BuildProbeTable {
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

  override def _importedClasses() = Set(
    "org.neo4j.collection.primitive.PrimitiveLongIntMap",
    "org.neo4j.collection.primitive.hopscotch.LongKeyIntValueTable")

  def producedType: String = "PrimitiveLongIntMap"

  def members() = ""

  def valueType = "int"

  override def operatorId: Some[String] = Some(id)

  override def generateFetchCode = {
    val timesSeen = namer.newVarName()
    val eventVar = s"event_$id"
    val code  = (key: String, action: Instruction) => {
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
    CodeThunk(Map.empty, code)
  }
}

case class CodeThunk(vars: Map[String, JavaSymbol], generator: (String, Instruction) => String) {
  def apply(key: String, instruction: Instruction) = generator(key, instruction)
}
