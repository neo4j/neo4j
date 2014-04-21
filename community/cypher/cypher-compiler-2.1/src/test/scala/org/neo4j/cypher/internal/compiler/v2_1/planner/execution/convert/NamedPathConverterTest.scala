/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution.convert

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.ProjectedPath
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NamedRelPath
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NamedNodePath

class NamedPathConverterTest extends CypherFunSuite {

  import ProjectedPath._

  implicit def idName(name: String): IdName = IdName(name)

  test("p = (a)") {
    val namedPath = NamedNodePath("p", "a")

    namedPathConverter(namedPath) should equal(
      ProjectedPath(
        Set("a"),
        singleNodeProjector("a", nilProjector)
      )
    )
  }

  test("p = (b)<-[r]-(a)") {
    val namedPath = NamedRelPath(IdName("p"), Seq(
      PatternRelationship("r", ("b", "a"), Direction.INCOMING, Seq.empty, SimplePatternLength)
    ))

    namedPathConverter(namedPath) should equal(
      ProjectedPath(
        Set("r", "b", "a"),
        singleNodeProjector("b",
          singleIncomingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r]->(b)") {
    val namedPath = NamedRelPath(IdName("p"), Seq(
      PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    ))

    namedPathConverter(namedPath) should equal(
      ProjectedPath(
        Set("r", "a", "b"),
        singleNodeProjector("a",
          singleOutgoingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (b)<-[r*1..]-(a)") {
    val namedPath = NamedRelPath(IdName("p"), Seq(
      PatternRelationship("r", ("b", "a"), Direction.INCOMING, Seq.empty, VarPatternLength(1, None))
    ))

    namedPathConverter(namedPath) should equal(
      ProjectedPath(
        Set("r", "b", "a"),
        singleNodeProjector("b",
          varLengthIncomingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r*1..]->(b)") {
    val namedPath = NamedRelPath(IdName("p"), Seq(
      PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, VarPatternLength(1, None))
    ))

    namedPathConverter(namedPath) should equal(
      ProjectedPath(
        Set("r", "a", "b"),
        singleNodeProjector("a",
          varLengthOutgoingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r*1..2]->(b)<-[r2]-c") {
    val namedPath = NamedRelPath(IdName("p"), Seq(
      PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, VarPatternLength(1, Some(2))),
      PatternRelationship("r2", ("b", "c"), Direction.INCOMING, Seq.empty, SimplePatternLength)
    ))

    namedPathConverter(namedPath) should equal(
      ProjectedPath(
        Set("r1", "a", "b", "r2", "c"),
        singleNodeProjector("a",
          varLengthOutgoingRelationshipProjector("r1",
            singleIncomingRelationshipProjector("r2", nilProjector)
          )
        )
      )
    )
  }
}
