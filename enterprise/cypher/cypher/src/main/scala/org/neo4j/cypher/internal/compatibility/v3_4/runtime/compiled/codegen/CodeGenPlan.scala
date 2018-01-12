/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

