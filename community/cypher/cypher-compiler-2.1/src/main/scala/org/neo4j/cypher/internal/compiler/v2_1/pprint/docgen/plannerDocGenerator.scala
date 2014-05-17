/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{PatternRelationship, SimplePatternLength, VarPatternLength, IdName}
import org.neo4j.cypher.internal.compiler.v2_1.ast.RelTypeName
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Selections, Predicate}

case object plannerDocGenerator extends NestedDocGenerator[Any] {

  import Doc._

  val forNestedIdName: RecursiveDocGenerator[Any] = {
    case idName: IdName => (inner) =>
      text(idName.name)
  }

  // TODO: This should go to ast doc generator
  val forNestedRelTypeName: RecursiveDocGenerator[Any] = {
    case relTypeName: RelTypeName => (inner) =>
      text(relTypeName.name)
  }

  val forNestedPatternLength: RecursiveDocGenerator[Any] = {
    case VarPatternLength(min, None)      => (inner) => text(s"*${min.toString}..")
    case VarPatternLength(min, Some(max)) => (inner) => text(s"*${min.toString}..${max.toString}")
    case SimplePatternLength              => (inner) => nil
  }

  val forNestedPatternRelationship: RecursiveDocGenerator[Any] = {
    case patRel: PatternRelationship => (inner) =>
      val leftEnd = if (patRel.dir == Direction.INCOMING) "<-[" else "-["
      val rightEnd = if (patRel.dir == Direction.OUTGOING) "]->" else "]-"

      group(
        "(" :: inner(patRel.left) :: ")" :: leftEnd ::
        inner(patRel.name) ::
        relTypeList(patRel.types)(inner) ::
        inner(patRel.length) :: rightEnd ::
        "(" :: inner(patRel.right) :: ")"
      )
  }

  val forNestedPredicate: RecursiveDocGenerator[Any] = {
    case Predicate(dependencies, expr) => (inner) =>

      val pred = sepList(dependencies.map(inner))
      val predBlock = block("Predicate", open = "[", close = "]")(pred)
      block(predBlock)(inner(expr))
  }

  val forNestedSelections: RecursiveDocGenerator[Any] = {
    case Selections(predicates) => (inner) =>
      sepList(predicates.map(inner).toList)
  }

  def relTypeList(list: Seq[RelTypeName])(inner: DocGenerator[Any]): Doc = list.map(inner).foldRight(nil) {
    case (hd, NilDoc) => ":" :: hd
    case (hd, tail)   => ":" :: hd :: "|" :: tail
  }

  protected val instance =
    forNestedIdName orElse
    forNestedRelTypeName orElse
    forNestedPatternLength orElse
    forNestedPatternRelationship orElse
    forNestedPredicate orElse
    forNestedSelections orElse
    queryProjectionDocGenerator("WITH") orElse
    queryGraphDocGenerator orElse
    plannerQueryDocGenerator
}
