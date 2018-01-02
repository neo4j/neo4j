/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.frontend.v2_3.CypherException
import org.neo4j.cypher.internal.frontend.v2_3.spi.MapToPublicExceptions
import org.neo4j.kernel.api.exceptions.Status

class CantCompileQueryException(message: String = "Internal error - should have used fall back to execute query, but something went horribly wrong", cause:Throwable=null)
  extends CypherException(message, cause) {

  def status = Status.Statement.ExecutionFailure

  def mapToPublic[T <: Throwable](thrower: MapToPublicExceptions[T]) = throw new CantCompileQueryException(message)
}
