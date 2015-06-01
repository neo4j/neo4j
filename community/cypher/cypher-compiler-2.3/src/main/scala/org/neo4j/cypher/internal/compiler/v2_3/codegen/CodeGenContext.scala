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
package org.neo4j.cypher.internal.compiler.v2_3.codegen

import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.JoinData
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v2_3.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan

import scala.collection.{immutable, mutable}

// STATEFUL!
class CodeGenContext(val semanticTable: SemanticTable, idMap: immutable.Map[LogicalPlan, Id]) {

  private val variables: mutable.Map[String, String] = mutable.Map()
  private val projections: mutable.Map[String, CodeGenExpression] = mutable.Map()
  private val probeTables: mutable.Map[CodeGenPlan, JoinData] = mutable.Map()
  private val parents: mutable.Stack[CodeGenPlan] = mutable.Stack()
  val operatorIds: mutable.Map[Id, String] = mutable.Map()

  val namer = Namer()

  def addVariable(name: String, symbol: String) {
    variables.put(name, symbol)
  }

  def addProjection(name: String, projection: CodeGenExpression) {
    projections.put(name, projection)
  }

  def getVariable(name: String): String = variables(name)

  def getProjection(name: String): CodeGenExpression = projections(name)

  def variableNames(): Set[String] = variables.keySet.toSet

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
