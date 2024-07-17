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

import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.DeprecatedPropertyReferenceInMerge
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.DeprecatedTextIndexProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CypherDeprecationNotificationsProviderTest extends CypherFunSuite {

  test("should filter out non-deprecation notifications") {
    val provider = CypherDeprecationNotificationsProvider.fromIterables(
      queryOptionsOffset = InputPosition(1, 2, 3),
      Set(
        DeprecatedFunctionNotification(InputPosition.NONE, "old", Some("new")),
        CartesianProductNotification(InputPosition.NONE, Set("x", "y"), ""),
        DeprecatedRelTypeSeparatorNotification(InputPosition.NONE, "old", "rewritten"),
        DeprecatedTextIndexProvider(InputPosition.NONE),
        DeprecatedPropertyReferenceInMerge(InputPosition.NONE, "prop"),
        UnboundedShortestPathNotification(InputPosition.NONE, "")
      )
    )

    val builder = Seq.newBuilder[String]
    provider.forEachDeprecation((name, _) => builder += name)

    builder.result() should contain theSameElementsAs Seq(
      "DeprecatedFunctionNotification",
      "DeprecatedRelTypeSeparatorNotification",
      "DeprecatedTextIndexProvider",
      "DeprecatedPropertyReferenceInMerge"
    )
  }
}
