/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.codegen

import org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.JoinData
import org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_1.SemanticTable

import scala.collection.mutable

case class Variable(name: String, codeGenType: CodeGenType, nullable: Boolean = false)

class CodeGenContext(val semanticTable: SemanticTable, idMap: Map[LogicalPlan, Id], val namer: Namer = Namer()) {

  private val variables: mutable.Map[String, Variable] = mutable.Map()
  private val probeTables: mutable.Map[CodeGenPlan, JoinData] = mutable.Map()
  private val parents: mutable.Stack[CodeGenPlan] = mutable.Stack()
  val operatorIds: mutable.Map[Id, String] = mutable.Map()

  def addVariable(queryVariable: String, variable: Variable) {
    variables.put(queryVariable, variable)
  }

  def getVariable(queryVariable: String): Variable = variables(queryVariable)

  def variableQueryVariables(): Set[String] = variables.keySet.toSet

  def addProbeTable(plan: CodeGenPlan, codeThunk: JoinData) {
    probeTables.put(plan, codeThunk)
  }

  def getProbeTable(plan: CodeGenPlan): JoinData = probeTables(plan)

  def pushParent(plan: CodeGenPlan) {
    if (plan.isInstanceOf[LeafCodeGenPlan]) {
      throw new IllegalArgumentException(s"Leafs can't be parents: $plan")
    }
    parents.push(plan)
  }

  def popParent(): CodeGenPlan = parents.pop()

  def registerOperator(plan: LogicalPlan): String = {
    operatorIds.getOrElseUpdate(idMap(plan), namer.newOpName(plan.getClass.getSimpleName))
  }
}
