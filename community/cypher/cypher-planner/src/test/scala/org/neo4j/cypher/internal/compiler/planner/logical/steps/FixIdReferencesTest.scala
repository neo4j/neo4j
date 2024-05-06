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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class FixIdReferencesTest extends CypherFunSuite {

  test("should rewrite IDs correctly with a new ID space") {
    val reasons = Seq(
      EagernessReason.Conflict(Id(1), Id(2)),
      EagernessReason.Conflict(Id(3), Id(4))
    )

    val mappingTable = Seq(
      Id(1) -> Id(2),
      Id(2) -> Id(3),
      Id(3) -> Id(4),
      Id(4) -> Id(5)
    )

    val fixer = new FixIdReferences(CancellationChecker.neverCancelled())

    mappingTable.foreach {
      case (old, nw) => fixer.registerMapping(old, nw)
    }

    reasons.endoRewrite(fixer(recursiveIdLookup = false)) should equal(
      Seq(
        EagernessReason.Conflict(Id(2), Id(3)),
        EagernessReason.Conflict(Id(4), Id(5))
      )
    )
  }

  test("should rewrite IDs correctly with the same ID space") {
    val reasons = Seq(
      EagernessReason.Conflict(Id(1), Id(2)),
      EagernessReason.Conflict(Id(3), Id(4))
    )

    val mappingTable = Seq(
      Id(1) -> Id(5),
      Id(2) -> Id(6),
      Id(3) -> Id(7),
      Id(4) -> Id(8)
    )

    val fixer = new FixIdReferences(CancellationChecker.neverCancelled())

    mappingTable.foreach {
      case (old, nw) => fixer.registerMapping(old, nw)
    }

    reasons.endoRewrite(fixer(recursiveIdLookup = true)) should equal(
      Seq(
        EagernessReason.Conflict(Id(5), Id(6)),
        EagernessReason.Conflict(Id(7), Id(8))
      )
    )
  }

  test("should rewrite IDs correctly with the same ID space and multiple remappings, and non-mapped IDs") {
    val reasons = Seq(
      EagernessReason.Conflict(Id(1), Id(2)),
      EagernessReason.Conflict(Id(3), Id(4))
    )

    val mappingTable = Seq(
      Id(1) -> Id(5),
      Id(3) -> Id(7),
      Id(5) -> Id(8),
      Id(8) -> Id(9),
      Id(7) -> Id(10)
    )

    val fixer = new FixIdReferences(CancellationChecker.neverCancelled())

    mappingTable.foreach {
      case (old, nw) => fixer.registerMapping(old, nw)
    }

    reasons.endoRewrite(fixer(recursiveIdLookup = true)) should equal(
      Seq(
        EagernessReason.Conflict(Id(9), Id(2)),
        EagernessReason.Conflict(Id(10), Id(4))
      )
    )
  }
}
