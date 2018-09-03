/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.expressions.Property
import org.opencypher.v9_0.util.{Rewriter, topDown}

case class replacePropertyLookupsWithVariables(availablePropertyVariables: Map[Property, CachedNodeProperty])  {

  /**
    * Rewrites any object to replace property lookups with variables, if they are available.
    * Registers these new variables with the given semantic table and returns a copy
    * of that semantic table where the new variables are known.
    */
  def apply(that: AnyRef, semanticTable: SemanticTable): (AnyRef, SemanticTable) = {
    var currentTypes = semanticTable.types

    val rewriter = topDown(Rewriter.lift {
      case property:Property if availablePropertyVariables.contains(property) =>
        val newVar = availablePropertyVariables(property)
        // Register the new variables in the semantic table
        currentTypes = currentTypes.updated(newVar, currentTypes(property))
        newVar
    })

    val rewritten = rewriter(that)
    val newSemanticTable = if(currentTypes == semanticTable.types) semanticTable else semanticTable.copy(types = currentTypes)
    (rewritten, newSemanticTable)
  }
}

object replacePropertyLookupsWithVariables {

  /**
    * Cast the first argument of the tuple to the desired type.
    */
  def firstAs[T <: AnyRef](arg: (AnyRef, SemanticTable)): (T, SemanticTable) = (arg._1.asInstanceOf[T], arg._2)
}
