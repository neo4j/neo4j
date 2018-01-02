/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.{Pretty, RecipeAppender}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection

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

  implicit class idNameConverter(idName: IdName) extends Converter {
    def unquote = AstNameConverter(idName.name).unquote
  }

  implicit class predicateConverter(predicate: Predicate) extends Converter {
    def unquote = {
      val pred = sepList(predicate.dependencies.toSeq.sorted(IdName.byName).map(pretty[IdName]), break = silentBreak)
      val predBlock = block("Predicate", open = "[", close = "]")(pred)
      group("Predicate" :: brackets(pred, break = noBreak) :: parens(pretty(predicate.expr)))
    }
  }

  implicit class selectionsConverter(selections: Selections) extends Converter {
    def unquote =
      sepList(selections.predicates.toSeq.sorted(Predicate.byPosition).map(pretty[Predicate]))
  }

  implicit class patternLengthConverter(patLength: PatternLength) extends Converter {
    def unquote = patLength match {
      case VarPatternLength(min, None)      => s"*${min.toString}.."
      case VarPatternLength(min, Some(max)) => s"*${min.toString}..${max.toString}"
      case SimplePatternLength              => nothing
    }
  }

  implicit class patternRelationshipConverter(patRel: PatternRelationship) extends Converter {
    def unquote = {
      val leftEnd = if (patRel.dir == SemanticDirection.INCOMING) "<-[" else "-["
      val rightEnd = if (patRel.dir == SemanticDirection.OUTGOING) "]->" else "]-"

      group(
        "(" :: pretty(patRel.left) :: ")" :: leftEnd ::
          pretty(patRel.name) ::
          relTypeList(patRel.types) ::
          pretty(patRel.length) :: rightEnd ::
          "(" :: pretty(patRel.right) :: ")"
      )
    }

    private def relTypeList(list: Seq[RelTypeName]): RecipeAppender[Any] = {
      if (list.isEmpty)
        nothing
      else {
        val prettyList = list.map(pretty[RelTypeName])
        ":" :: sepList(prettyList, sep = "|", break = noBreak)
      }
    }
  }

  implicit class shortestPathConverter(sp: ShortestPathPattern) extends Converter {
    def unquote = {
      val nameDoc = sp.name.fold[RecipeAppender[Any]](nothing)(name => name.name :: " =")
      val relDoc = block(if (sp.single) "shortestPath" else "allShortestPath")(pretty(sp.rel))
      nameDoc :/?: relDoc
    }
  }

  implicit class queryShuffleConverter(shuffle: QueryShuffle) extends Converter {
    def unquote = {
      val sortItemDocs = shuffle.sortItems.map(pretty[SortItem])
      val sortItems = if (sortItemDocs.isEmpty) nothing else group("ORDER BY" :/: sepList(sortItemDocs))
      val skip = shuffle.skip.map(skip => group("SKIP" :/: pretty[Expression](skip))).getOrElse(nothing)
      val limit = shuffle.limit.map(limit => group("LIMIT" :/: pretty[Expression](limit))).getOrElse(nothing)

      sortItems :/?: skip :/?: limit
    }
  }

  implicit class queryGraphConverter(qg: QueryGraph) extends Converter {
    def unquote = {
      val argIds = qg.argumentIds.toSeq.sorted(IdName.byName)
      val args = section("GIVEN")(if (argIds.isEmpty) "*" else sepList(qg.argumentIds.map(pretty[IdName])))
      val patterns = section("MATCH")(sepList(
        qg.patternNodes.toSeq.sorted(IdName.byName).map(id => "(" :: pretty(id) :: ")") ++
          qg.patternRelationships.toSeq.sorted(PatternRelationship.byName).map(pretty[PatternRelationship]) ++
          qg.shortestPathPatterns.toSeq.sorted(ShortestPathPattern.byRelName).map(pretty[ShortestPathPattern])
      ))

      val optionalMatches = qg.optionalMatches.sorted(QueryGraph.byCoveredIds).map(pretty[QueryGraph])
      val optional =
        if (optionalMatches.isEmpty) nothing
        else section("OPTIONAL")(block("", open = "{ ", close = " }")(sepList(optionalMatches)))

      val selections = qg.selections
      val where = if (selections.isEmpty) nothing else section("WHERE")(pretty(selections))

      val hints = breakList(qg.hints.toSeq.sorted(Hint.byIdentifier

      ).map(pretty[Hint]))

      group(args :/?: patterns :/?: optional :/?: hints :/?: where)
    }
  }

  implicit class plannerQueryConverter(pq: PlannerQuery) extends Converter {
    def unquote = {
      val allQueryDocs = queryDocs(Some(pq), List.empty)
      val brokenDocs = breakList(allQueryDocs)
      group(brokenDocs)
    }

    @tailrec
    private final def queryDocs(optQuery: Option[PlannerQuery], docs: List[RecipeAppender[Any]]): List[RecipeAppender[Any]] = {
      optQuery match {
        case None => docs.reverse
        case Some(query) => queryDocs(query.tail, queryDoc(query) :: docs)
      }
    }

    private def queryDoc(query: PlannerQuery): RecipeAppender[Any] = {
      val graphDoc = pretty(query.graph)

      // This is a hack:
      // tail should move into QueryHorizon in PlannerQuery. This way pprinting horizons can figure out if it's
      // a WITH or a RETURN

      val projectionDoc = query.horizon match {
        case unwind: UnwindProjection =>
          pretty(unwind)
        case queryProjection: QueryProjection =>
          val projectionPrefix = query.tail.fold("RETURN")(_ => "WITH")
          section(projectionPrefix)(queryProjection.unquote)
      }
      group(graphDoc :/: projectionDoc)
    }
  }

  implicit class QueryHorizonConverter(horizon: QueryHorizon) extends Converter {
    def unquote = horizon match {
      case queryProjection: AggregatingQueryProjection =>
        val projectionDoc = generateDoc(queryProjection.projections ++ queryProjection.aggregationExpressions, queryProjection.shuffle)
        if (queryProjection.aggregationExpressions.isEmpty) "DISTINCT" :/: group(projectionDoc) else projectionDoc

      case queryProjection: QueryProjection =>
        generateDoc(queryProjection.projections, queryProjection.shuffle)

      case queryProjection: UnwindProjection =>
        section("UNWIND")(generateDoc(Map(queryProjection.identifier.name -> queryProjection.exp), QueryShuffle.empty))
    }

    def generateDoc(projections: Map[String, Expression], queryShuffle: QueryShuffle): RecipeAppender[Any] = {
        val projectionMapDoc = projections.toSeq.sortBy(_._1).collect {
          case (k, v) => group(pretty(v) :/: "AS " :: s"`$k`")
        }

        val projectionDoc = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc))
        val shuffleDoc = queryShuffle.unquote

        val sortItemDocSeq = queryShuffle.sortItems.collect {
          case AscSortItem(expr) => pretty[Expression](expr)
          case DescSortItem(expr) => pretty[Expression](expr) :/: "DESC"
        }
        val sortItemDoc = if (sortItemDocSeq.isEmpty) nothing else group("ORDER BY" :/: sepList(sortItemDocSeq))

        val skipDoc = queryShuffle.skip.map(skip => group("SKIP" :/: pretty(skip))).getOrElse(nothing)
        val limitDoc = queryShuffle.limit.map(limit => group("LIMIT" :/: pretty(limit))).getOrElse(nothing)

        projectionDoc :/?: shuffleDoc
    }
  }
}
