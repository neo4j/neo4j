/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.LockDescription
import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, PropertyKeyName}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.{IdName, QueryGraph}

class PlanUpdatesTest extends CypherFunSuite with AstConstructionTestSupport {

  // MATCH (x:X) MERGE (x)-[:T]->(:W {p3: 666})-[:S]->(:T {apa: 'yes'})
  // :X  {p1: 42, p2: 46}
  // :Y  {p1: 42, p2: 46}
  // :W  {p3: 666}

  val prop = PropertyKeyName("prop")(pos)
  val prop2 = PropertyKeyName("prop2")(pos)

  test("single property is handled") {
    val predicates = QueryGraph.empty.addPredicates(
      hasLabels("x", "LABEL"),
      propEquality("x", "prop", 42)
    ).addPatternNodes(IdName("x"))

    val lockingDescriptions = PlanUpdates.createLockingDescriptions(predicates)
    lockingDescriptions should equal(Seq(LockDescription(lblName("LABEL"), Seq(prop -> literalInt(42)))))
  }

  test("multiple properties is handled correctly") {
    val predicates = QueryGraph.empty.addPredicates(
      hasLabels("x", "LABEL"),
      propEquality("x", "prop", 42),
      propEquality("x", "prop2", 666)
    ).addPatternNodes(IdName("x"))

    val lockingDescriptions = PlanUpdates.createLockingDescriptions(predicates)
    lockingDescriptions should equal(Seq(LockDescription(lblName("LABEL"), Seq(prop -> literalInt(42), prop2 -> literalInt(666)))))
  }

  test("two labels are handled as expected") {
    val predicates = QueryGraph.empty.addPredicates(
      hasLabels("x", "LABEL"),
      hasLabels("x", "LABEL2"),
      propEquality("x", "prop", 42)
    ).addPatternNodes(IdName("x"))

    val lockingDescriptions = PlanUpdates.createLockingDescriptions(predicates)
    lockingDescriptions should equal(Seq(
      LockDescription(lblName("LABEL"), Seq(prop -> literalInt(42))),
      LockDescription(lblName("LABEL2"), Seq(prop -> literalInt(42)))))
  }

  test("two nodes are handled well") {
    val predicates = QueryGraph.empty.addPredicates(
      hasLabels("x", "LABEL"),
      hasLabels("y", "LABEL2"),
      propEquality("x", "prop", 42),
      propEquality("y", "prop2", 666)
    ).addPatternNodes(IdName("x"), IdName("y"))

    val lockingDescriptions = PlanUpdates.createLockingDescriptions(predicates)
    lockingDescriptions should equal(Seq(
      LockDescription(lblName("LABEL"), Seq(prop -> literalInt(42))),
      LockDescription(lblName("LABEL2"), Seq(prop2 -> literalInt(666)))))
  }
}
