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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.ast.{AstConstructionTestSupport, False, True}
import org.neo4j.cypher.internal.compiler.v2_2.perty.gen.DocHandlerTestSuite
import org.neo4j.cypher.internal.compiler.v2_2.perty.handler.SimpleDocHandler
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Predicate, Selections}

class PlannerParticleDocGenTest extends DocHandlerTestSuite[Any] with AstConstructionTestSupport {

  val docGen = plannerParticleDocGen ++ SimpleDocHandler.docGen

  test("IdName(a) => a") {
    pprintToString(IdName("a")) should equal("a")
  }

  test("Predicate[a,b](True)") {
    pprintToString(Predicate(Set(IdName("a"), IdName("b")), True()_)) should equal("Predicate[a,b](True)")
  }

  test("Selections(Predicate[a](True), Predicate[b](False)) = Predicate[a](True), Predicate[b](False)") {
    val phrase = Selections(Set(
      Predicate(Set(IdName("a")), True()_),
      Predicate(Set(IdName("b")), False()_)
    ))

    val result = pprintToString(phrase)

    result should equal("Predicate[a](True), Predicate[b](False)")
  }
}
