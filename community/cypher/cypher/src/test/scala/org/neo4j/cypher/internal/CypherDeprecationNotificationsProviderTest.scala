/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.DeprecatedCreateIndexSyntax
import org.neo4j.cypher.internal.util.DeprecatedFunctionNotification
import org.neo4j.cypher.internal.util.DeprecatedParameterSyntax
import org.neo4j.cypher.internal.util.DeprecatedRelTypeSeparatorNotification
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.MissingAliasNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CypherDeprecationNotificationsProviderTest extends CypherFunSuite {

  test("should filter out non-deprecation notifications") {
    val provider = CypherDeprecationNotificationsProvider(
      queryOptionsOffset = InputPosition(1, 2, 3),
      preParserNotifications = Set(
        DeprecatedFunctionNotification(InputPosition.NONE, "old", "new"),
        CartesianProductNotification(InputPosition.NONE, Set("x", "y")),
        DeprecatedRelTypeSeparatorNotification(InputPosition.NONE)
      ),
      otherNotifications = Set(
        DeprecatedParameterSyntax(InputPosition.NONE),
        DeprecatedCreateIndexSyntax(InputPosition.NONE),
        MissingAliasNotification(InputPosition.NONE)
      )
    )

    val builder = Seq.newBuilder[String]
    provider.forEachDeprecation((name, _) => builder += name)

    builder.result() should contain theSameElementsAs Seq(
        "DeprecatedFunctionNotification",
        "DeprecatedRelTypeSeparatorNotification",
        "DeprecatedParameterSyntax",
        "DeprecatedCreateIndexSyntax"
    )
  }
}
