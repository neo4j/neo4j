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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.JoinData
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.v3_4.logical.plans.{LogicalPlan, LogicalPlanId}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable

import scala.collection.mutable

case class Variable(name: String, codeGenType: CodeGenType, nullable: Boolean = false)

class CodeGenContext(val semanticTable: SemanticTable,
                     lookup: Map[String, Int], val namer: Namer = Namer()) {

  private val variables: mutable.Map[String, Variable] = mutable.Map()
  private val projectedVariables: mutable.Map[String, Variable] = mutable.Map.empty
  private val probeTables: mutable.Map[CodeGenPlan, JoinData] = mutable.Map()
  private val parents: mutable.Stack[CodeGenPlan] = mutable.Stack()
  val operatorIds: mutable.Map[LogicalPlanId, String] = mutable.Map()

  def addVariable(queryVariable: String, variable: Variable) {
    //assert(!variables.isDefinedAt(queryVariable)) // TODO: Make the cases where overwriting the value is ok explicit (by using updateVariable)
    variables.put(queryVariable, variable)
  }

  def numberOfColumns() = lookup.size

  def nameToIndex(name: String) = lookup.getOrElse(name, throw new InternalException(s"$name is not a mapped column"))

  def updateVariable(queryVariable: String, variable: Variable) {
    assert(variables.isDefinedAt(queryVariable))
    variables.put(queryVariable, variable)
  }

  def getVariable(queryVariable: String): Variable = variables(queryVariable)

  def hasVariable(queryVariable: String): Boolean = variables.isDefinedAt(queryVariable)

  def isProjectedVariable(queryVariable: String): Boolean = projectedVariables.contains(queryVariable)

  def variableQueryVariables(): Set[String] = variables.keySet.toSet

  // We need to keep track of variables that are exposed by a QueryHorizon,
  // e.g. Projection, Unwind, ProcedureCall, LoadCsv
  // These variables are the only ones that needs to be considered for materialization by an eager operation, e.g. Sort
  def addProjectedVariable(queryVariable: String, variable: Variable) {
    projectedVariables.put(queryVariable, variable)
  }

  // We need to keep only the projected variables that are exposed by a QueryHorizon, e.g a regular Projection
  def retainProjectedVariables(queryVariablesToRetain: Set[String]): Unit = {
    projectedVariables.retain((key, _) => queryVariablesToRetain.contains(key))
  }

  def getProjectedVariables: Map[String, Variable] = projectedVariables.toMap

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
    operatorIds.getOrElseUpdate(plan.assignedId, namer.newOpName(plan.getClass.getSimpleName))
  }
}

object CodeGenContext {
  def sanitizedName(name: String) = name.replace(" ", "_")
}
