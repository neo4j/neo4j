/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.ir.v3_4.{IdName, PlannerQuery}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, ReadOnlies, Solveds, TransactionLayers}
import org.neo4j.cypher.internal.util.v3_4.Cardinality
import org.neo4j.cypher.internal.util.v3_4.attribution.{Id, SequentialIdGen}

import scala.language.implicitConversions

trait LogicalPlanConstructionTestSupport extends CypherTestSupport {
  implicit val idGen = new SequentialIdGen()
  implicit protected def idName(name: String): IdName = IdName(name)
  implicit protected def idSymbol(name: Symbol): IdName = IdName(name.name)

  class FakeSolveds extends Solveds {
    override def set(id: Id, t: PlannerQuery): Unit = {}
    override def isDefinedAt(id: Id): Boolean = true
    override def get(id: Id): PlannerQuery = PlannerQuery.empty
    override def copy(from: Id, to: Id): Unit = {}
  }

  class FakeReadOnlies extends ReadOnlies {
    override def set(id: Id, t: Boolean): Unit = {}
    override def isDefinedAt(id: Id): Boolean = true
    override def get(id: Id): Boolean = true
    override def copy(from: Id, to: Id): Unit = {}
  }

  class FakeCardinalities extends Cardinalities {
    override def set(id: Id, t: Cardinality): Unit = {}
    override def isDefinedAt(id: Id): Boolean = true
    override def get(id: Id): Cardinality = 0.0
    override def copy(from: Id, to: Id): Unit = {}
  }

  class FakeTransactionLayers extends TransactionLayers {
    override def set(id: Id, t: Int): Unit = {}
    override def isDefinedAt(id: Id): Boolean = true
    override def get(id: Id): Int = 0
    override def copy(from: Id, to: Id): Unit = {}
  }
}
