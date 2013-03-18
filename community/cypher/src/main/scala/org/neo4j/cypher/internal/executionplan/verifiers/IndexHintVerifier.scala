/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.verifiers

import org.neo4j.cypher.internal.commands.{Equals, SchemaIndex, Query, AbstractQuery}
import org.neo4j.cypher.internal.commands.expressions.{Identifier, Property}
import org.neo4j.cypher.{SyntaxException, IndexHintException}

object IndexHintVerifier extends Verifier {
  override val verifyFunction: PartialFunction[AbstractQuery, Unit] = {
    case query: Query =>
      val predicateAtoms = query.where.atoms

      if ( (! query.hints.isEmpty) && (! query.start.isEmpty) )
        throw new SyntaxException("Cannot use index hints with start clause")

      query.hints.foreach {
        case SchemaIndex(id, label, prop, _) =>

          val valid = predicateAtoms.exists(_ match {
            case Equals(Property(Identifier(identifier), property), _) => id == identifier && property == prop
            case _                                                     => false
          })

          if (!valid)
            throw new IndexHintException(id, label, prop,
              "Can't use an index hint without an equality comparison on the correct node property label combo.")
      }
  }
}