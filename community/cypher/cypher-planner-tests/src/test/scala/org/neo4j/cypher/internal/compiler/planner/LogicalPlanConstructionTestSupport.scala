/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.planner.spi.PlanningAttributesTestStubs
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

import scala.language.implicitConversions

trait LogicalPlanConstructionTestSupport extends PlanningAttributesTestStubs {
  self: AstConstructionTestSupport =>

  implicit val idGen: IdGen = new SequentialIdGen()

  implicit protected def idSymbol(name: Symbol): String = name.name

  def propReadSetConflict(prop: String, id1: Int, id2: Int): EagernessReason.ReasonWithConflict =
    EagernessReason.PropertyReadSetConflict(propName(prop)).withConflict(EagernessReason.Conflict(Id(id1), Id(id2)))

  def readDeleteConflict(entity: String, id1: Int, id2: Int): EagernessReason.ReasonWithConflict =
    EagernessReason.ReadDeleteConflict(entity).withConflict(EagernessReason.Conflict(Id(id1), Id(id2)))
}
