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

object PlannerDocGenerator extends NestedDocGenerator[Any] {

  import Doc._

  val forNestedIdName: RecursiveDocGenerator[Any] = {
    case idName: IdName => (inner: DocGenerator[Any]) =>
      text(idName.name)
  }

  // TODO: This should go to ast doc generator
  val forNestedRelTypeName: RecursiveDocGenerator[Any] = {
    case relTypeName: RelTypeName => (inner: DocGenerator[Any]) =>
      text(relTypeName.name)
  }

  val forNestedPatternLength: RecursiveDocGenerator[Any] = {
    case VarPatternLength(min, None)      => (inner: DocGenerator[Any]) => text(s"*${min.toString}..")
    case VarPatternLength(min, Some(max)) => (inner: DocGenerator[Any]) => text(s"*${min.toString}..${max.toString}")
    case SimplePatternLength              => (inner: DocGenerator[Any]) => nil
  }

  val forNestedPatternRelationship: RecursiveDocGenerator[Any] = {
    case patRel: PatternRelationship => (inner: DocGenerator[Any]) =>
      val leftEnd = if (patRel.dir == Direction.INCOMING) "<-[" else "-["
      val rightEnd = if (patRel.dir == Direction.OUTGOING) "]->" else "]-"

      group(list(List(
        text("("), inner(patRel.left), text(")"),
        text(leftEnd),
        inner(patRel.name),
        relTypeList(patRel.types)(inner),
        inner(patRel.length),
        text(rightEnd),
        text("("), inner(patRel.right), text(")")
      )))
  }

  val forNestedPredicate: RecursiveDocGenerator[Any] = {
    case Predicate(dependencies, expr) => (inner: DocGenerator[Any]) =>

      scalaDocGroup(
        scalaGroup("Predicate", open = "[", close = "]")(List(
          sepList(dependencies.map(inner).toList)
        )))(List(inner(expr))
        )
  }

  val forNestedSelections: RecursiveDocGenerator[Any] = {
    case Selections(predicates) => (inner: DocGenerator[Any]) =>
      sepList(predicates.map(inner).toList)
  }

  def starList[T](list: List[T])(inner: DocGenerator[Any]) =
    if (list.isEmpty)
      text("*")
    else
      sepList(list.map(inner))

  def relTypeList(list: Seq[RelTypeName])(inner: DocGenerator[Any]): Doc = list.map(inner).foldRight(nil) {
    case (hd, NilDoc) => cons(text(":"), cons(hd))
    case (hd, tail)   => cons(text(":"), cons(hd, cons(text("|"), cons(tail))))
  }

  protected val instance =
    forNestedIdName orElse
    forNestedRelTypeName orElse
    forNestedPatternLength orElse
    forNestedPatternRelationship orElse
    forNestedPredicate orElse
    forNestedSelections orElse
    QueryProjectionDocGenerator("WITH") orElse
    QueryGraphDocGenerator orElse
    PlannerQueryDocGenerator
}
