/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.spec.interpreted

import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.tests.ProfileDbHitsTestBase
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

object LegacyDbHitsTestBase {
  final val costOfExpandGetRelCursor: Long = 1 // to get the rel cursor
  final val costOfExpandOneRel: Long = 1 // to get one relationship in a rel cursor
}

abstract class LegacyDbHitsTestBase[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int)
  extends ProfileDbHitsTestBase(edition,
                                runtime,
                                sizeHint,
                                costOfLabelLookup = 1,
                                costOfGetPropertyChain = 0,
                                costOfPropertyJumpedOverInChain = 0,
                                costOfProperty = 1,
                                costOfExpandGetRelCursor = LegacyDbHitsTestBase.costOfExpandOneRel,
                                costOfExpandOneRel = LegacyDbHitsTestBase.costOfExpandOneRel,
                                costOfRelationshipTypeLookup = 1,
                                cartesianProductChunkSize = 1)
