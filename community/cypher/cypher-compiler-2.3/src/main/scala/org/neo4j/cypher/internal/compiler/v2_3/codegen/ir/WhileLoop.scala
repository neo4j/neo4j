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

case class WhileLoop(id: JavaSymbol, producer: LoopDataGenerator, action: Instruction) extends Instruction {

  def generateCode(): String = {
    val iterator = s"${id.name}Iter"
    val eventVar = s"event_${producer.id}"

    s"""try ( QueryExecutionEvent $eventVar = tracer.executeOperator( ${producer.id} ) )
       |{
       |${producer.javaType} $iterator = ${producer.generateCode()};
       |$eventVar.dbHit();
       |while ( $iterator.hasNext() )
       |{
       |$eventVar.dbHit();
       |$eventVar.row();
       |final ${id.javaType} ${id.name} = $iterator.next();
       |${producer.generateVariablesAndAssignment()}
       |${action.generateCode()}
       |}
       |}
       |""".stripMargin
  }

  override protected def importedClasses = Set(
    "org.neo4j.collection.primitive.PrimitiveLongIterator",
    "org.neo4j.collection.primitive.Primitive")

  override def children = Seq(producer, action)

  // Initialises necessary data-structures. Is inserted at the top of the generated method
  def generateInit() = producer.generateInit() + n + action.generateInit()

  def members() = producer.members() + n + action.members()
}
