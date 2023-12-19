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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.JoinTableType
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

trait CodeGenPlan {

  val logicalPlan: LogicalPlan

  def produce(context: CodeGenContext, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction])

  def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction])
}

trait LeafCodeGenPlan extends CodeGenPlan {

  override final def consume(context: CodeGenContext, child: CodeGenPlan, cardinalities: Cardinalities): (Option[JoinTableMethod], List[Instruction]) =
    throw new UnsupportedOperationException("Leaf plan does not consume")
}

case class JoinTableMethod(name: String, tableType: JoinTableType)

