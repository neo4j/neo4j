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
package org.neo4j.cypher.internal

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.ObfuscationPolicy.FullLiteralsAlways
import org.neo4j.cypher.internal.ObfuscationPolicy.FullLiteralsOnDemand
import org.neo4j.cypher.internal.ObfuscationPolicy.SensitiveLiteralsOnly

class ObfuscationPolicyTest extends CommunityCypherTestSuite {

  test("fromConfig maps obfuscate_literals=true to FullLiteralsAlways regardless of the fail-safe") {
    ObfuscationPolicy.fromConfig(obfuscateLiterals = true, exposeFullView = true) should equal(FullLiteralsAlways)
    ObfuscationPolicy.fromConfig(obfuscateLiterals = true, exposeFullView = false) should equal(FullLiteralsAlways)
  }

  test("fromConfig maps obfuscate_literals=false + fail-safe on to FullLiteralsOnDemand") {
    ObfuscationPolicy.fromConfig(obfuscateLiterals = false, exposeFullView = true) should equal(FullLiteralsOnDemand)
  }

  test("fromConfig maps obfuscate_literals=false + fail-safe off to SensitiveLiteralsOnly") {
    ObfuscationPolicy.fromConfig(obfuscateLiterals = false, exposeFullView = false) should equal(SensitiveLiteralsOnly)
  }

  test("SensitiveLiteralsOnly exposes no full-literals view and is not the default") {
    SensitiveLiteralsOnly.fullLiteralsAvailable shouldBe false
    SensitiveLiteralsOnly.fullLiteralsByDefault shouldBe false
  }

  test("FullLiteralsOnDemand makes the full-literals view available but not the default") {
    FullLiteralsOnDemand.fullLiteralsAvailable shouldBe true
    FullLiteralsOnDemand.fullLiteralsByDefault shouldBe false
  }

  test("FullLiteralsAlways makes the full-literals view available and the default") {
    FullLiteralsAlways.fullLiteralsAvailable shouldBe true
    FullLiteralsAlways.fullLiteralsByDefault shouldBe true
  }
}
