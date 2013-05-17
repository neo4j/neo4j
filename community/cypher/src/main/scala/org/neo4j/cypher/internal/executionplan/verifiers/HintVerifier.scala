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

import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.commands.expressions.Identifier
import org.neo4j.cypher.{LabelScanHintException, IndexHintException, SyntaxException}
import org.neo4j.cypher.internal.commands.Equals
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.internal.commands.expressions.Property

object HintVerifier extends Verifier {
  override val verifyFunction: PartialFunction[AbstractQuery, Unit] = {
    case query: Query =>
      val predicateAtoms = query.where.atoms

      if ((!query.hints.isEmpty) && (!query.start.isEmpty))
        throw new SyntaxException("Cannot use index hints with start clause")

      query.hints.foreach {
        case NodeByLabel(hintId, hintLabel) =>
          val valid = predicateAtoms.exists {
            case HasLabel(Identifier(predicateId), predicateLabel) =>
              predicateId == hintId && predicateLabel.name == hintLabel

            case x                                                 =>
              false
          }
          if (!valid)
            throw new LabelScanHintException(hintId, hintLabel,
              "Can't use a label scan hint without using the label for that identifier in your +MATCH+ or +WHERE+")


        case SchemaIndex(id, label, prop, _) =>

          val valid = predicateAtoms.exists {
            case Equals(Property(Identifier(identifier), property), _) => id == identifier && property == prop
            case _                                                     => false
          }

          if (!valid)
            throw new IndexHintException(id, label, prop,
              "Can't use an index hint without an equality comparison on the correct node property label combo.")
      }
  }
}