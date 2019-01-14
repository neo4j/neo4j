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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.JoinData
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

import scala.collection.mutable

case class Variable(name: String, codeGenType: CodeGenType, nullable: Boolean = false)

class CodeGenContext(val semanticTable: SemanticTable,
                     lookup: Map[String, Int], val namer: Namer = Namer()) {

  private val variables: mutable.Map[String, Variable] = mutable.Map()
  private val projectedVariables: mutable.Map[String, Variable] = mutable.Map.empty
  private val probeTables: mutable.Map[CodeGenPlan, JoinData] = mutable.Map()
  private val parents: mutable.Stack[CodeGenPlan] = mutable.Stack()
  val operatorIds: mutable.Map[Id, String] = mutable.Map()

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
    operatorIds.getOrElseUpdate(plan.id, namer.newOpName(plan.getClass.getSimpleName))
  }
}

object CodeGenContext {
  def sanitizedName(name: String) = name.replace(" ", "_")
}
