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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ast.{HasLabels, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.QueryGraphProducer
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship, SimplePatternLength}
import org.neo4j.graphdb.Direction

class ProducePredicatesTest extends CypherFunSuite with LogicalPlanningTestSupport with QueryGraphProducer {
  test("single node produces no predicates") {
    predicatesFor("MATCH a") should equal(Set())
  }

  test("single node with labels produces one predicate") {
    predicatesFor("MATCH (a:Person)") should equal(Set(
      ExpressionPredicate(HasLabels(ident("a"), Seq(LabelName("Person") _)) _)
    ))
  }

  test("pattern with one relationship produces one predicate") {
    predicatesFor("MATCH a-[r]->(b)") should equal(Set(
      PatternPredicate(PatternRelationship(IdName("r"), (IdName("a"), IdName("b")), Direction.OUTGOING, Seq.empty, SimplePatternLength))
    ))
  }

  private def predicatesFor(q: String) =
    producePredicates(
      produceQueryGraphForPattern(q))
}
