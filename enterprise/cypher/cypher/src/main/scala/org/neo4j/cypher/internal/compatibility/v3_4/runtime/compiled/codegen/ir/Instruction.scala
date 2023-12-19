/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure

trait Instruction {
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = children.foreach(_.init(generator))
  def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  protected def children: Seq[Instruction]

  private def treeView: Seq[Instruction] = {
    children.foldLeft(Seq(this)) { (acc, child) => acc ++ child.treeView }
  }

  // Aggregating methods -- final to prevent overriding
  final def allOperatorIds: Set[String] = treeView.flatMap(_.operatorId).toSet

  protected def operatorId: Set[String] = Set.empty
}

object Instruction {

  val empty = new Instruction {
    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

    override protected def children = Seq.empty

    override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}
  }
}
