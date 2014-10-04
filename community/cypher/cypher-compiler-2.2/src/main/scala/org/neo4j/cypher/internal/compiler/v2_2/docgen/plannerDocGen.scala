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
import org.neo4j.cypher.internal.compiler.v2_2.perty.helpers.HasType
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.{RecipeAppender, Pretty}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

import scala.annotation.tailrec

import scala.reflect.runtime.universe.TypeTag

case object plannerDocGen extends CustomDocGen[Any] {

  import Pretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
    case idName: IdName => idName.asPretty
    case predicate: Predicate => predicate.asPretty
    case selections: Selections => selections.asPretty
    case patLength: PatternLength => patLength.asPretty
    case patRel: PatternRelationship => patRel.asPretty
    case sp: ShortestPathPattern => sp.asPretty
    case shuffle: QueryShuffle => shuffle.asPretty
    case qg: QueryGraph => qg.asPretty
    case pq: PlannerQuery => pq.asPretty
    case horizon: QueryHorizon => horizon.asPretty
    case _ => None
  }

  implicit class idNameConverter(idName: IdName) {
    def asPretty: Option[DocRecipe[Any]] = AstNameConverter(idName.name).asPretty
  }

  implicit class predicateConverter(predicate: Predicate) {
    def asPretty: Option[DocRecipe[Any]] = {
      val pred = sepList(predicate.dependencies.map(pretty[IdName]), break = silentBreak)
      val predBlock = block("Predicate", open = "[", close = "]")(pred)
      Pretty(group("Predicate" :: brackets(pred, break = noBreak) :: parens(pretty(predicate.expr))))
    }
  }

  implicit class selectionsConverter(selections: Selections) {
    def asPretty: Option[DocRecipe[Any]] = {
      Pretty(sepList(selections.predicates.map(pretty[Predicate])))
    }
  }

  implicit class patternLengthConverter(patLength: PatternLength) {
    def asPretty: Option[DocRecipe[Any]] = patLength match {
      case VarPatternLength(min, None)      => Pretty(s"*${min.toString}..")
      case VarPatternLength(min, Some(max)) => Pretty(s"*${min.toString}..${max.toString}")
      case SimplePatternLength              => Pretty(nothing)
    }
  }

  implicit class patternRelationshipConverter(patRel: PatternRelationship) {
    def asPretty: Option[DocRecipe[Any]] = {
      val leftEnd = if (patRel.dir == Direction.INCOMING) "<-[" else "-["
      val rightEnd = if (patRel.dir == Direction.OUTGOING) "]->" else "]-"

      Pretty(group(
        "(" :: pretty(patRel.left) :: ")" :: leftEnd ::
          pretty(patRel.name) ::
          relTypeList(patRel.types) ::
          pretty(patRel.length) :: rightEnd ::
          "(" :: pretty(patRel.right) :: ")"
      ))
    }

    def relTypeList(list: Seq[RelTypeName]): DocRecipe[Any] = {
      if (list.isEmpty)
        Pretty(nothing)
      else {
        val prettyList = list.map(pretty[RelTypeName])
        Pretty(":" :: sepList(prettyList, sep = "|", break = noBreak))
      }
    }
  }

  implicit class shortestPathConverter(sp: ShortestPathPattern) {
    def asPretty: Option[DocRecipe[Any]] = {
      val nameDoc = sp.name.fold[RecipeAppender[Any]](nothing)(name => name.name :: " =")
      val relDoc = block(if (sp.single) "shortestPath" else "allShortestPath")(pretty(sp.rel))
      Pretty(nameDoc :/?: relDoc)
    }
  }

  implicit class queryShuffleConverter(shuffle: QueryShuffle) {
    def asPretty: Option[DocRecipe[Any]] = {
      val sortItemDocs = shuffle.sortItems.map(pretty[SortItem])
      val sortItems = if (sortItemDocs.isEmpty) nothing else group("ORDER BY" :/: sepList(sortItemDocs))
      val skip = shuffle.skip.map(skip => group("SKIP" :/: pretty[Expression](skip))).getOrElse(nothing)
      val limit = shuffle.limit.map(limit => group("LIMIT" :/: pretty[Expression](limit))).getOrElse(nothing)

      Pretty(sortItems :/?: skip :/?: limit)
    }
  }

  implicit class queryGraphConverter(qg: QueryGraph) {
    def asPretty: Option[DocRecipe[Any]] = {
      val argIds = qg.argumentIds
      val args = section("GIVEN")(if (argIds.isEmpty) "*" else sepList(qg.argumentIds.map(pretty[IdName])))
      val patterns = section("MATCH")(sepList(
        qg.patternNodes.map(id => "(" :: pretty(id) :: ")") ++
          qg.patternRelationships.map(pretty[PatternRelationship]) ++
          qg.shortestPathPatterns.map(pretty[ShortestPathPattern])
      ))

      val optionalMatches = qg.optionalMatches.map(pretty[QueryGraph])
      val optional =
        if (optionalMatches.isEmpty) nothing
        else section("OPTIONAL")(block("", open = "{ ", close = " }")(sepList(optionalMatches)))

      val selections = qg.selections
      val where = if (selections.isEmpty) nothing else section("WHERE")(pretty(selections))

      val hints = breakList(qg.hints.map(pretty[Hint]))

      Pretty(group(args :/?: patterns :/?: optional :/?: hints :/?: where))
    }
  }

  implicit class plannerQueryConverter(pq: PlannerQuery) {
    def asPretty: Option[DocRecipe[Any]] = {
      val allQueryDocs = queryDocs(Some(pq), List.empty)
      Pretty(group(list(allQueryDocs)))
    }

    @tailrec
    final def queryDocs(optQuery: Option[PlannerQuery], docs: List[RecipeAppender[Any]]): List[RecipeAppender[Any]] = {
      optQuery match {
        case None => docs.reverse
        case Some(query) => queryDocs(query.tail, queryDoc(query) :: docs)
      }
    }

    def queryDoc(query: PlannerQuery): RecipeAppender[Any] = {
      val graphDoc = pretty(query.graph)

      // This is a hack:
      // tail should move into QueryHorizon in PlannerQuery. This way pprinting horizons can figure out if it's
      // a WITH or a RETURN

      val projectionDoc = query.horizon match {
        case unwind: UnwindProjection =>
          pretty(unwind)
        case queryProjection: QueryProjection =>
          val projectionPrefix = query.tail.fold("RETURN")(_ => "WITH")
          section(projectionPrefix)(pretty(queryProjection))
      }
      group(graphDoc :/: projectionDoc)
    }
  }

  implicit class QueryHorizonConverter(horizon: QueryHorizon) {
    def asPretty: Option[DocRecipe[Any]] = {
      val appender = horizon match {
        case queryProjection: AggregatingQueryProjection =>
          val projectionDoc = generateDoc(queryProjection.projections ++ queryProjection.aggregationExpressions, queryProjection.shuffle)
          if (queryProjection.aggregationExpressions.isEmpty) "DISTINCT" :/: group(projectionDoc) else projectionDoc

        case queryProjection: QueryProjection =>
          generateDoc(queryProjection.projections, queryProjection.shuffle)

        case queryProjection: UnwindProjection =>
          section("UNWIND")(generateDoc(Map(queryProjection.identifier.name -> queryProjection.exp), QueryShuffle.empty))
      }
      Pretty(appender)
    }

    def generateDoc(projections: Map[String, Expression], queryShuffle: QueryShuffle): RecipeAppender[Any] = {
        val projectionMapDoc = projections.collect {
          case (k, v) => group(pretty(v) :/: "AS " :: s"`$k`")
        }

        val projectionDoc = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc))
        val shuffleDoc = pretty(queryShuffle)

        val sortItemDocs = queryShuffle.sortItems.collect {
          case AscSortItem(expr) => pretty(expr)
          case DescSortItem(expr) => pretty(expr) :/: "DESC"
        }
        val sortItems = if (sortItemDocs.isEmpty) nothing else group("ORDER BY" :/: sepList(sortItemDocs))

        val skip = queryShuffle.skip.map(skip => group("SKIP" :/: pretty(skip))).getOrElse(nothing)
        val limit = queryShuffle.limit.map(limit => group("LIMIT" :/: pretty(limit))).getOrElse(nothing)

        projectionDoc :/?: shuffleDoc
    }
  }
}
