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
package org.neo4j.router.util

import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.messages.MessageUtilProvider
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.fabric.planning.Use
import org.neo4j.kernel.database.DatabaseReference

object Errors {

  def syntax(msg: String): Nothing = throw new SyntaxException(msg)

  def semantic(message: String) = throw new InvalidSemanticsException(message)

  private def dynamicGraphNotAllowedMessage(use: String) =
    MessageUtilProvider.createDynamicGraphReferenceUnsupportedError(use).stripMargin

  def dynamicGraphNotAllowed(use: GraphSelection): Nothing =
    syntax(dynamicGraphNotAllowedMessage(Use.show(use)))

  def cantAccessOutsideCompositeMessage(target: DatabaseReference, sessionDatabase: DatabaseReference): Nothing =
    throw new InvalidSemanticsException(
      s"When connected to a composite database, access is allowed only to its constituents. " +
        s"Attempted to access '${target.toPrettyString}' while connected to '${sessionDatabase.toPrettyString}'"
    );

}
