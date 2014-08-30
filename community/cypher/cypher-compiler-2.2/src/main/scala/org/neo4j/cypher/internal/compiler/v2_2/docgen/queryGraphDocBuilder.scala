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
//import org.neo4j.cypher.internal.compiler.v2_2.perty._
//import org.neo4j.cypher.internal.compiler.v2_2.perty.impl.CachingDocBuilder
//import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
//
//case object queryGraphDocBuilder extends CustomDocBuilder[Any] {
//
//  import Doc._
//
//  override def newDocGenerator = DocGenerator {
//    case qg: QueryGraph => (inner) =>
//      val args = section("GIVEN", "*" :?: sepList(qg.argumentIds.map(inner)))
//      val patterns = section("MATCH", sepList(
//        qg.patternNodes.map(id => "(" :: inner(id) :: ")") ++
//        qg.patternRelationships.map(inner) ++
//        qg.shortestPathPatterns.map(inner)
//      ))
//
//      val optionalMatches = qg.optionalMatches.map(inner)
//      val optional =
//        if (optionalMatches.isEmpty) nil
//        else section("OPTIONAL", block("", open="{ ", close=" }")(sepList(optionalMatches)))
//
//      val where = section("WHERE", inner(qg.selections))
//
//      val hints = breakList(qg.hints.map(inner))
//
//      group(args :+: patterns :+: optional :+: hints :+: where)
//  }
//}
