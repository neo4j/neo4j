/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.verifiers

import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Identifier
import org.neo4j.cypher.{LabelScanHintException, IndexHintException, SyntaxException}
import org.neo4j.cypher.internal.compiler.v2_0.commands.Equals
import org.neo4j.cypher.internal.compiler.v2_0.commands.SchemaIndex
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Property
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.StringHelper.RichString

object HintVerifier extends Verifier {
  override val verifyFunction: PartialFunction[AbstractQuery, Unit] = {

    case query: Query =>
      val predicateAtoms = query.where.atoms

      if ((!query.hints.isEmpty) && (!query.start.isEmpty))
        throw new SyntaxException("Cannot use index hints with start clause")

      def containsLabel(identifier: String, label: String) = {
        val existsInPredicates = predicateAtoms.exists {
          case HasLabel(Identifier(predicateId), predicateLabel) =>
            predicateId == identifier && predicateLabel.name == label

          case x =>
            false
        }

        def hasLabelInPattern(node: SingleNode): Boolean = node.name == identifier && node.labels.exists(_.name == label)

        val existsInPattern = query.matching.exists {
          case node: SingleNode             => hasLabelInPattern(node)
          case RelationshipPattern(_, a, b) => hasLabelInPattern(a) || hasLabelInPattern(b)
        }

        existsInPattern || existsInPredicates
      }

      def hasExpectedPredicate(id: String, prop: String) = predicateAtoms.exists {
        case Equals(Property(Identifier(identifier), property), _) => id == identifier && property.name == prop
        case Equals(_, Property(Identifier(identifier), property)) => id == identifier && property.name == prop
        case _                                                     => false
      }

      query.hints.foreach {
        case NodeByLabel(hintIdentifier, hintLabel) if !containsLabel(hintIdentifier, hintLabel) =>
          throw new LabelScanHintException(hintIdentifier, hintLabel,
            """|Cannot use label scan hint in this context.
               | Label scan hints require using a simple label test in WHERE (either directly or as part of a
               | top-level AND). Note that the label must be specified on a non-optional node""".stripLinesAndMargins)

        case SchemaIndex(id, label, prop, _, _) if !hasExpectedPredicate(id, prop) || !containsLabel(id, label) =>
          throw new IndexHintException(id, label, prop,
            """|Cannot use index hint in this context.
               | Index hints require using a simple equality comparison in WHERE (either directly or as part of a
               | top-level AND).
               | Note that the label and property comparison must be specified on a
               | non-optional node""".stripLinesAndMargins)
        case _ =>
      }
  }

}
