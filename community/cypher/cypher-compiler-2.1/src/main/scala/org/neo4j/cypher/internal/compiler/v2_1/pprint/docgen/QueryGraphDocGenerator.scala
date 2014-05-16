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
import org.neo4j.cypher.internal.compiler.v2_1.planner.{Selections, Predicate, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{VarPatternLength, SimplePatternLength, PatternRelationship, IdName}
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.RelTypeName

object QueryGraphDocGenerator {

  import Doc._

  val forIdName: RecursiveDocGenerator[Any] = {
    case idName: IdName => (inner: DocGenerator[Any]) =>
      text(idName.name)
  }

  val forRelTypeName: RecursiveDocGenerator[Any] = {
    case relTypeName: RelTypeName => (inner: DocGenerator[Any]) =>
      text(relTypeName.name)
  }

  val forPatternLength: RecursiveDocGenerator[Any] = {
    case VarPatternLength(min, None)      => (inner: DocGenerator[Any]) => text(s"*${min.toString}..")
    case VarPatternLength(min, Some(max)) => (inner: DocGenerator[Any]) => text(s"*${min.toString}..${max.toString}")
    case SimplePatternLength              => (inner: DocGenerator[Any]) => nil
  }

  val forPatternRelationship: RecursiveDocGenerator[Any] = {
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

  val forPredicate: RecursiveDocGenerator[Any] = {
    case Predicate(dependencies, expr) => (inner: DocGenerator[Any]) =>

      scalaDocGroup(
        scalaGroup("Predicate", open = "[", close = "]")(List(
          sepList(dependencies.map(inner).toList)
        )))(List(inner(expr))
      )
  }

  val forSelections: RecursiveDocGenerator[Any] = {
    case Selections(predicates) => (inner: DocGenerator[Any]) =>
      sepList(predicates.map(inner).toList)
  }

    val forQueryGraph: RecursiveDocGenerator[Any] = {
    case qg: QueryGraph => (inner: DocGenerator[Any]) =>
      val args = section("GIVEN", starList(qg.argumentIds.toList)(inner))
      val patterns = section("MATCH", sepList(
        qg.patternNodes.map(id => cons(text("("), cons(inner(id), cons(")")))).toList ++
        qg.patternRelationships.map(inner).toList
      ))

      val optionalMatches = qg.optionalMatches.map(inner).toList
      val optional =
        if (optionalMatches.isEmpty) nil
        else section("OPTIONAL", scalaGroup("", open="{ ", close=" }")(optionalMatches))

      val where = section("WHERE", inner(qg.selections))

      group(breakList(List(args, patterns, optional, where).filter(_ != NilDoc)))
  }

  val forPlannerTypes =
    forIdName orElse
    forRelTypeName orElse
    forPatternLength orElse
    forPatternRelationship orElse
    forPredicate orElse
    forSelections orElse
    forQueryGraph

  private def section(start: String, inner: Doc): Doc = inner match {
    case NilDoc => nil
    case _      => group(breakCons(text(start), nest(inner)))
  }

  private def starList[T](list: List[T])(inner: DocGenerator[Any]) =
    if (list.isEmpty)
      text("*")
    else
      sepList(list.map(inner))

  private def relTypeList(list: Seq[RelTypeName])(inner: DocGenerator[Any]): Doc = list.map(inner).foldRight(nil) {
    case (hd, NilDoc) => cons(text(":"), cons(hd))
    case (hd, tail)   => cons(text(":"), cons(hd, cons(text("|"), cons(tail))))
  }
}
