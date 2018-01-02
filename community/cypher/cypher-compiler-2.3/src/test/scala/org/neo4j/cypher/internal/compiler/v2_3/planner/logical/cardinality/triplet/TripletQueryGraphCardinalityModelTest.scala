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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.{ForumPostsCardinalityData, RandomizedCardinalityModelTestSuite}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics

class TripletQueryGraphCardinalityModelTest extends RandomizedCardinalityModelTestSuite with ForumPostsCardinalityData {

  ignore("MATCH (person:Person {id: 10})-[r1:KNOWS]-(friend:Person)<-[r2:MEMBER_IN]-(forum:Forum) WHERE r2.count>1234 AND not(person=friend)") {
    import ForumPosts._

    val persons = Persons * PersonIdSel
    val friends = Persons
    val forums = Forums

    val personKnowsDeg = degree( Persons_KNOWS_STAR, Persons ) + degree( STAR_KNOWS_Persons, Persons )
    val r1Left = personKnowsDeg * persons
    val r1Right = personKnowsDeg * friends
    val r1 = Math.min(r1Left, r1Right)

    val personMemberInDeg = degree( STAR_MEMBER_IN_Persons, Persons )
    val forumMemberOutDeg = degree( Forums_MEMBER_IN_STAR, Forums )
    val r2Left = forumMemberOutDeg * forums
    val r2Right = personMemberInDeg * friends
    val r2 = Math.min(r2Left, r2Right)

    val tripletCross = r1 * r2

    val overlap = friends

    val unselected = tripletCross / overlap

    val selectivity = DEFAULT_RANGE_SELECTIVITY * (1.0d - DEFAULT_EQUALITY_SELECTIVITY)
    val result = unselected * selectivity

    forQuery("MATCH (person:Person {id: 10})-[r1:KNOWS]-(friend:Person)<-[r2:MEMBER_IN]-(forum:Forum) WHERE r2.count>1234 AND not(person=friend)").
      shouldHaveQueryGraphCardinality(result)
  }

  def createCardinalityModel(stats: GraphStatistics): QueryGraphCardinalityModel =
    TripletQueryGraphCardinalityModel(stats, combiner)
}
