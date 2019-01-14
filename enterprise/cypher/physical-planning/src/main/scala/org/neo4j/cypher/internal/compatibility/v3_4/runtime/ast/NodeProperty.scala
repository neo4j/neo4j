/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
