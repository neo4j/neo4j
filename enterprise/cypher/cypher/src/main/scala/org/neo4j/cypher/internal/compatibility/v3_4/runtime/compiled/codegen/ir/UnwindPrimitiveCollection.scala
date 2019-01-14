/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenExpression, CodeGenType, CypherCodeGenType, ListReferenceType}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.util.v3_4.symbols

case class UnwindPrimitiveCollection(opName: String, collection: CodeGenExpression) extends LoopDataGenerator {
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit =
    collection.init(generator)

  override def produceLoopData[E](iterVar: String, generator: MethodStructure[E])
                                 (implicit context: CodeGenContext): Unit = {
    generator.declarePrimitiveIterator(iterVar, collection.codeGenType)
    val iterator = generator.primitiveIteratorFrom(collection.generateExpression(generator), collection.codeGenType)
    generator.assign(iterVar, CodeGenType.Any, iterator)
  }

  override def getNext[E](nextVar: Variable, iterVar: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext): Unit = {
    val elementType = collection.codeGenType match {
      case CypherCodeGenType(symbols.ListType(innerCt), ListReferenceType(innerRepr)) => CypherCodeGenType(innerCt, innerRepr)
      case _ => throw new IllegalArgumentException(s"CodeGenType $collection.codeGenType not supported as primitive iterator")
    }
    val next = generator.primitiveIteratorNext(generator.loadVariable(iterVar), collection.codeGenType)
    generator.assign(nextVar.name, elementType, next)
  }

  override def checkNext[E](generator: MethodStructure[E], iterVar: String): E =
    generator.iteratorHasNext(generator.loadVariable(iterVar))

  override def close[E](iterVarName: String,
                        generator: MethodStructure[E]): Unit = {/*nothing to close*/}
}
