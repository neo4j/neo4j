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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

import scala.annotation.tailrec

case object plannerDocGen { // extends CustomDocGen[Any] {

  import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._

  def drill = ???
//  mkDocDrill[Any]() {
//    // ast objects (temporary, shouldn't be here, only exist for tests)
//    case Identifier(name) => inner => AstNameConverter(name).asDoc
//    case _: CountStar => inner => "count(*)"
//
//    // planner objects
//    case idName: IdName => inner => idName.asDoc
//    case predicate: Predicate => inner => predicate.asDoc(inner)
//    case selections: Selections => inner => selections.asDoc(inner)
//    case patLength: PatternLength => inner => patLength.asDoc
//    case patRel: PatternRelationship => inner => patRel.asDoc(inner)
//    case sp: ShortestPathPattern => inner => sp.asDoc(inner)
//    case shuffle: QueryShuffle => inner => shuffle.asDoc(inner)
//    case qg: QueryGraph => inner => qg.asDoc(inner)
//    case pq: PlannerQuery => inner => pq.asDoc(inner)
//    case horizon: QueryHorizon => inner => horizon.asDoc(inner)
//  }

//  implicit class idNameConverter(idName: IdName) {
//    def asDoc = AstNameConverter(idName.name).asDoc
//  }
//
//  implicit class predicateConverter(predicate: Predicate) {
//    def asDoc(pretty: DocConverter[Any]) = {
//      val pred = sepList(predicate.dependencies.map(pretty), break = breakSilent)
//      val predBlock = block("Predicate", open = "[", close = "]")(pred)
//      group("Predicate" :: brackets(pred, break = noBreak) :: parens(pretty(predicate.expr)))
//    }
//  }
//
//  implicit class selectionsConverter(selections: Selections) {
//    def asDoc(pretty: DocConverter[Any]) =
//      sepList(selections.predicates.map(pretty))
//  }
//
//  implicit class patternLengthConverter(patLength: PatternLength) {
//    def asDoc: Doc = patLength match {
//        case VarPatternLength(min, None)      => s"*${min.toString}.."
//        case VarPatternLength(min, Some(max)) => s"*${min.toString}..${max.toString}"
//        case SimplePatternLength              => nil
//      }
//  }
//
//  implicit class patternRelationshipConverter(patRel: PatternRelationship) {
//    def asDoc(pretty: DocConverter[Any]) = {
//      val leftEnd = if (patRel.dir == Direction.INCOMING) "<-[" else "-["
//      val rightEnd = if (patRel.dir == Direction.OUTGOING) "]->" else "]-"
//
//      group(
//        "(" :: pretty(patRel.left) :: ")" :: leftEnd ::
//          pretty(patRel.name) ::
//          relTypeList(patRel.types)(pretty) ::
//          pretty(patRel.length) :: rightEnd ::
//          "(" :: pretty(patRel.right) :: ")"
//      )
//    }
//
//    def relTypeList(list: Seq[RelTypeName])(inner: DocConverter[Any]): Doc = {
//      if (list.isEmpty) nil
//      else {
//        val prettyList = list map (inner)
//        ":" :: sepList(prettyList, sep = "|", break = noBreak)
//      }
//    }
//  }
//
//  implicit class shortestPathConverter(sp: ShortestPathPattern) {
//    def asDoc(pretty: DocConverter[Any]): Doc = {
//      val nameDoc = sp.name.fold(nil)(name => name.name :: " =")
//      val relDoc = block(if (sp.single) "shortestPath" else "allShortestPath")(pretty(sp.rel))
//      nameDoc :+: relDoc
//    }
//  }
//
//  implicit class queryShuffleConverter(shuffle: QueryShuffle) {
//    def asDoc(pretty: DocConverter[Any]) = {
//      val sortItemDocs = shuffle.sortItems.map(pretty)
//      val sortItems = if (sortItemDocs.isEmpty) nil else group("ORDER BY" :/: sepList(sortItemDocs))
//      val skip = shuffle.skip.fold(nil)(skip => group("SKIP" :/: pretty(skip)))
//      val limit = shuffle.limit.fold(nil)(limit => group("LIMIT" :/: pretty(limit)))
//
//      sortItems :+: skip :+: limit
//    }
//  }
//
//  implicit class queryGraphConverter(qg: QueryGraph) {
//    def asDoc(pretty: DocConverter[Any]): Doc = {
//      val args = section("GIVEN", "*" :?: sepList(qg.argumentIds.map(pretty)))
//      val patterns = section("MATCH", sepList(
//        qg.patternNodes.map(id => "(" :: pretty(id) :: ")") ++
//          qg.patternRelationships.map(pretty) ++
//          qg.shortestPathPatterns.map(pretty)
//      ))
//
//      val optionalMatches = qg.optionalMatches.map(pretty)
//      val optional =
//        if (optionalMatches.isEmpty) nil
//        else section("OPTIONAL", block("", open = "{ ", close = " }")(sepList(optionalMatches)))
//
//      val where = section("WHERE", pretty(qg.selections))
//
//      val hints = breakList(qg.hints.map(pretty))
//
//      group(args :+: patterns :+: optional :+: hints :+: where)
//    }
//  }
//
//  implicit class plannerQueryConverter(pq: PlannerQuery) {
//    def asDoc(pretty: DocConverter[Any]): Doc = {
//      val allQueryDocs = queryDocs(pretty, Some(pq), List.empty)
//      group(breakList(allQueryDocs))
//    }
//
//    @tailrec
//    final def queryDocs(inner: DocConverter[Any], optQuery: Option[PlannerQuery], docs: List[Doc]): List[Doc] = {
//      optQuery match {
//        case None => docs.reverse
//        case Some(query) => queryDocs(inner, query.tail, queryDoc(inner, query) :: docs)
//      }
//    }
//
//    def queryDoc(inner: DocConverter[Any], query: PlannerQuery) = {
//      val graphDoc = inner(query.graph)
//
//      // This is a hack:
//      // tail should move into QueryHorizon in PlannerQuery. This way pprinting horizons can figure out if it's
//      // a WITH or a RETURN
//
//      val projectionDoc = query.horizon match {
//        case unwind: UnwindProjection =>
//          inner(unwind)
//        case queryProjection: QueryProjection =>
//          val projectionPrefix = query.tail.fold("RETURN")(_ => "WITH")
//          section(projectionPrefix, inner(queryProjection))
//      }
//      group(graphDoc :/: projectionDoc)
//    }
//  }
//
//  implicit class QueryHorizonConverter(horizon: QueryHorizon) {
//    def asDoc(pretty: DocConverter[Any]) = horizon match {
//      case queryProjection: AggregatingQueryProjection =>
//        val projectionDoc = generateDoc(pretty)(queryProjection.projections ++ queryProjection.aggregationExpressions, queryProjection.shuffle)
//        if (queryProjection.aggregationExpressions.isEmpty) "DISTINCT" :/: group(projectionDoc) else projectionDoc
//
//      case queryProjection: QueryProjection =>
//        generateDoc(pretty)(queryProjection.projections, queryProjection.shuffle)
//
//      case queryProjection: UnwindProjection =>
//        section("UNWIND", generateDoc(pretty)(Map(queryProjection.identifier.name -> queryProjection.exp), QueryShuffle.empty))
//    }
//
//    def generateDoc(pretty: DocConverter[Any])(projections: Map[String, Expression], queryShuffle: QueryShuffle): Doc = {
//        val projectionMapDoc = projections.collect {
//          case (k, v) => group(pretty(v) :/: "AS " :: s"`$k`")
//        }
//
//        val projectionDoc = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc))
//        val shuffleDoc = pretty(queryShuffle)
//
//        val sortItemDocs = queryShuffle.sortItems.collect {
//          case AscSortItem(expr) => pretty(expr)
//          case DescSortItem(expr) => pretty(expr) :/: "DESC"
//        }
//        val sortItems = if (sortItemDocs.isEmpty) nil else group("ORDER BY" :/: sepList(sortItemDocs))
//
//        val skip = queryShuffle.skip.fold(nil)(skip => group("SKIP" :/: pretty(skip)))
//        val limit = queryShuffle.limit.fold(nil)(limit => group("LIMIT" :/: pretty(limit)))
//
//        projectionDoc :+: shuffleDoc
//    }
//  }
}
