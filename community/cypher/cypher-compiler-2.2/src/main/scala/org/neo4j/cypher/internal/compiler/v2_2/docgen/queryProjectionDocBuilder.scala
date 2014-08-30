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
//import org.neo4j.cypher.internal.compiler.v2_2.ast.{AscSortItem, DescSortItem, Expression}
//import org.neo4j.cypher.internal.compiler.v2_2.perty._
//import org.neo4j.cypher.internal.compiler.v2_2.perty.impl.CachingDocBuilder
//import org.neo4j.cypher.internal.compiler.v2_2.planner.{AggregatingQueryProjection, QueryProjection, QueryShuffle, UnwindProjection}
//
//case class queryProjectionDocBuilder(prefix: String = "WITH") extends CustomDocBuilder[Any] {
//
//  import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
//
//  override def newDocGenerator = DocGenerator {
//    case queryProjection: AggregatingQueryProjection =>
//      val distinct = if (queryProjection.aggregationExpressions.isEmpty) "DISTINCT" else ""
//      generateDoc(queryProjection.projections ++ queryProjection.aggregationExpressions, queryProjection.shuffle, distinct, prefix)
//
//    case queryProjection: QueryProjection =>
//      generateDoc(queryProjection.projections, queryProjection.shuffle, "", prefix)
//
//    case queryProjection: UnwindProjection =>
//      generateDoc(Map(queryProjection.identifier.name -> queryProjection.exp), QueryShuffle.empty, "", "UNWIND")
//  }
//
//  private def generateDoc(projections: Map[String, Expression], queryShuffle: QueryShuffle, initialString: String, prefix: String): FixedDocGenerator[Any] => Doc = {
//    (inner: FixedDocGenerator[Any]) =>
//
//      val projectionMapDoc = projections.collect {
//        case (k, v) => group(inner(v) :/: "AS " :: s"`$k`")
//      }
//
//      val projectionDoc = if (projectionMapDoc.isEmpty) text("*") else group(sepList(projectionMapDoc))
//      val shuffleDoc = inner(queryShuffle)
//
//      val sortItemDocs = queryShuffle.sortItems.collect {
//        case AscSortItem(expr)  => inner(expr)
//        case DescSortItem(expr) => inner(expr) :/: "DESC"
//      }
//      val sortItems = if (sortItemDocs.isEmpty) nil else group("ORDER BY" :/: sepList(sortItemDocs))
//
//      val skip = queryShuffle.skip.fold(nil)(skip => group("SKIP" :/: inner(skip)))
//      val limit = queryShuffle.limit.fold(nil)(limit => group("LIMIT" :/: inner(limit)))
//
//      section(prefix, projectionDoc :+: shuffleDoc)
//  }
//}
