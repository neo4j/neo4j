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
package org.neo4j.cypher.internal.compiler.v2_3.birk.codegen

import org.neo4j.cypher.internal.compiler.v2_3.birk.JavaSymbol
import org.neo4j.cypher.internal.compiler.v2_3.birk.il.CodeThunk
import org.neo4j.cypher.internal.compiler.v2_3.planner.SemanticTable

import scala.collection.mutable

case class CodeGenContext(semanticTable: SemanticTable) {

  private val variables: mutable.Map[String, JavaSymbol] = mutable.Map()
  private val probeTables: mutable.Map[CodeGenPlan, CodeThunk] = mutable.Map()
  private val parents: mutable.Stack[CodeGenPlan] = mutable.Stack()

  val namer = Namer()

  def addVariable(name: String, symbol: JavaSymbol): Unit = variables.put(name, symbol)

  def getVariable(name: String): JavaSymbol = variables(name)

  def variableNames(): Set[String] = variables.keySet.toSet

  def addProbeTable(plan: CodeGenPlan, codeThunk: CodeThunk): Unit = probeTables.put(plan, codeThunk)

  def getProbeTable(plan: CodeGenPlan): CodeThunk = probeTables(plan)

  def pushParent(plan: CodeGenPlan): Unit = {
    if (plan.isInstanceOf[LeafCodeGenPlan]) {
      throw new IllegalArgumentException(s"Leafs can't be parents: $plan")
    }
    parents.push(plan)
  }

  def popParent(): CodeGenPlan = parents.pop()
}
