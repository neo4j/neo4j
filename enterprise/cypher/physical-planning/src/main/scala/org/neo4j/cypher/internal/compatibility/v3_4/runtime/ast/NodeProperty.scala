/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.ast

import org.neo4j.cypher.internal.v3_4.expressions.Property

case class NodeProperty(offset: Int, propToken: Int, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

// Token did not exist at plan time, so we'll need to look it up at runtime
case class NodePropertyLate(offset: Int, propKey: String, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

case class NodePropertyExists(offset: Int, propToken: Int, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}

// Token did not exist at plan time, so we'll need to look it up at runtime
case class NodePropertyExistsLate(offset: Int, propKey: String, name: String)(prop: Property) extends RuntimeProperty(prop) {
  override def asCanonicalStringVal: String = name
}
