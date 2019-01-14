/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v3_5.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v3_5.expressions._

trait PlanMatchHelp extends AstConstructionTestSupport {

  protected def cachedNodePropertyProj(node: String, property: String): (String, CachedNodeProperty) =
    s"$node.$property" -> cachedNodeProperty(node, property)

  protected def cachedNodePropertyProj(alias: String, node: String, property: String ): (String, CachedNodeProperty) =
    alias -> cachedNodeProperty(node, property)

  protected def cachedNodeProperty(node: String, property: String): CachedNodeProperty =
    CachedNodeProperty(node, PropertyKeyName(property)(pos))(pos)

  protected def propertyProj(node: String, property: String ): (String, Property) =
    s"$node.$property" -> Property(Variable(node)(pos), PropertyKeyName(property)(pos))(pos)
}
