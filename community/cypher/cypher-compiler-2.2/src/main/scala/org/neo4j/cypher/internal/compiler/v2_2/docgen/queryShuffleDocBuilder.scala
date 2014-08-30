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
//import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._
//import org.neo4j.cypher.internal.compiler.v2_2.perty.{CustomDocBuilder, DocGenerator}
//import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryShuffle
//
//object queryShuffleDocBuilder extends CustomDocBuilder[Any] {
//
//  override def newDocGenerator = DocGenerator {
//    case shuffle: QueryShuffle => (inner) =>
//      val sortItemDocs = shuffle.sortItems.map(inner)
//      val sortItems = if (sortItemDocs.isEmpty) nil else group("ORDER BY" :/: sepList(sortItemDocs))
//
//      val skip = shuffle.skip.fold(nil)(skip => group("SKIP" :/: inner(skip)))
//      val limit = shuffle.limit.fold(nil)(limit => group("LIMIT" :/: inner(limit)))
//
//      sortItems :+: skip :+: limit
//  }
//}
