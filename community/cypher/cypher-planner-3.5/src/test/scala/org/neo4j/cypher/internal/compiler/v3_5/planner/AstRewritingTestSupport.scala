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
package org.neo4j.cypher.internal.compiler.v3_5.planner

import org.neo4j.cypher.internal.ir.v3_5.PlannerQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.opencypher.v9_0.ast.AstConstructionTestSupport
import org.opencypher.v9_0.parser.ParserFixture
import org.opencypher.v9_0.util.Cardinality
import org.opencypher.v9_0.util.attribution.{Id, SequentialIdGen}
import org.opencypher.v9_0.util.test_helpers.CypherTestSupport

import scala.language.implicitConversions

trait LogicalPlanConstructionTestSupport extends CypherTestSupport {
  implicit val idGen = new SequentialIdGen()

  implicit protected def idSymbol(name: Symbol): String = name.name

  class StubSolveds extends Solveds {
    override def set(id: Id, t: PlannerQuery): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): PlannerQuery = PlannerQuery.empty

    override def copy(from: Id, to: Id): Unit = {}
  }

  class StubCardinalities extends Cardinalities {
    override def set(id: Id, t: Cardinality): Unit = {}

    override def isDefinedAt(id: Id): Boolean = true

    override def get(id: Id): Cardinality = 0.0

    override def copy(from: Id, to: Id): Unit = {}
  }

}

trait AstRewritingTestSupport extends CypherTestSupport with AstConstructionTestSupport {
  val parser = ParserFixture.parser
}
