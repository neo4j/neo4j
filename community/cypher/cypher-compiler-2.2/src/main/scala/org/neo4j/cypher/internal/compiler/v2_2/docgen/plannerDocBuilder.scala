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
///**
// * Copyright (c) 2002-2014 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program.  If not, see <http://www.gnu.org/licenses/>.
// */
//package org.neo4j.cypher.internal.compiler.v2_2.docbuilders
//
//import org.neo4j.cypher.internal.compiler.v2_2.ast.RelTypeName
//import org.neo4j.cypher.internal.compiler.v2_2.perty._
//import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
//import org.neo4j.cypher.internal.compiler.v2_2.planner.{Predicate, Selections}
//import org.neo4j.graphdb.Direction
//
//case object plannerDocBuilder extends DocBuilderChain[Any] {
//
//  import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
//  import org.neo4j.cypher.internal.compiler.v2_2.perty.DocBuilder._
//
//  val forNestedIdName = DocGenerator[Any] {
//    case idName: IdName => (inner) =>
//      text(idName.name)
//  }
//
//  val forNestedPatternLength: DocBuilder[Any] = DocGenerator[Any] {
//    case VarPatternLength(min, None)      => (inner) => text(s"*${min.toString}..")
//    case VarPatternLength(min, Some(max)) => (inner) => text(s"*${min.toString}..${max.toString}")
//    case SimplePatternLength              => (inner) => nil
//  }
//
//  val forNestedPatternRelationship: DocBuilder[Any] = DocGenerator[Any] {
//    case patRel: PatternRelationship => (inner) =>
//      val leftEnd = if (patRel.dir == Direction.INCOMING) "<-[" else "-["
//      val rightEnd = if (patRel.dir == Direction.OUTGOING) "]->" else "]-"
//
//      group(
//        "(" :: inner(patRel.left) :: ")" :: leftEnd ::
//        inner(patRel.name) ::
//        relTypeList(patRel.types)(inner) ::
//        inner(patRel.length) :: rightEnd ::
//        "(" :: inner(patRel.right) :: ")"
//      )
//  }
//
//  val forNestedPredicate: DocBuilder[Any] = DocGenerator[Any] {
//    case Predicate(dependencies, expr) => (inner) =>
//      val pred = sepList(dependencies.map(inner), break = breakSilent)
//      val predBlock = block("Predicate", open = "[", close = "]")(pred)
//      group("Predicate" :: brackets(pred, break = noBreak) :: parens(inner(expr)))
//  }
//
//  val forNestedSelections: DocBuilder[Any] = DocGenerator[Any] {
//    case Selections(predicates) => (inner) =>
//      sepList(predicates.map(inner).toList)
//  }
//
//  val forNestedShortestPathPattern: DocBuilder[Any] = DocGenerator[Any] {
//    case ShortestPathPattern(optName, rel, single) => (inner) =>
//      val nameDoc = optName.fold(nil)(name => name.name :: " =")
//      val relDoc = block(if (single) "shortestPath" else "allShortestPath")(inner(rel))
//      nameDoc :+: relDoc
//  }
//
//  def relTypeList(list: Seq[RelTypeName])(inner: FixedDocGenerator[Any]): Doc = list.map(inner).foldRight(nil) {
//    case (hd, NilDoc) => ":" :: hd
//    case (hd, tail)   => ":" :: hd :: "|" :: tail
//  }
//
//  val builders: Seq[DocBuilder[Any]] =
//    Seq(
//      forNestedIdName,
//      forNestedPatternLength,
//      forNestedPatternRelationship,
//      forNestedShortestPathPattern,
//      forNestedPredicate,
//      forNestedSelections,
//      queryGraphDocBuilder,
//      logicalPlanDocBuilder,
//      queryProjectionDocBuilder("WITH"),
//      queryShuffleDocBuilder,
//      plannerQueryDocBuilder
//    )
//}
