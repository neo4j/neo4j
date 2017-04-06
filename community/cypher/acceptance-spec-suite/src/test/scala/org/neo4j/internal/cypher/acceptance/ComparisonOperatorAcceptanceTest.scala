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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class ComparisonOperatorAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should handle long chains of comparisons") {
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 2 < 3 < 4 as val") shouldBe true
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 3 < 2 < 4 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 2 < 2 < 4 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 2 <= 2 < 4 as val") shouldBe true

    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 2.1 < 3.2 < 4.2 as val") shouldBe true
    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 3.2 < 2.1 < 4.2 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 2.1 < 2.1 < 4.2 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 2.1 <= 2.1 < 4.2 as val") shouldBe true

    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'b' < 'c' < 'd' as val") shouldBe true
    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'c' < 'b' < 'd' as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'b' < 'b' < 'd' as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'b' <= 'b' < 'd' as val") shouldBe true
  }
}
