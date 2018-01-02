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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult

class TriadicIntegrationTest extends ExecutionEngineFunSuite {
  private val QUERY: String = """MATCH (p1:Person)-[:FRIEND]-()-[:FRIEND]-(p2:Person)
                                |WHERE NOT (p1)-[:FRIEND]-(p2)
                                |RETURN p1.name AS l, p2.name AS r""".stripMargin
  test("find friends of others") {
    // given
    execute( """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c"}), (d:Person{name:"d"})
               |CREATE (a)-[:FRIEND]->(c), (b)-[:FRIEND]->(c), (c)-[:FRIEND]->(d)""".stripMargin)

    // when
    val result = profile(QUERY)

    // then
    result.toSet should equal(Set(
      Map("l" -> "a", "r" -> "b"),
      Map("l" -> "a", "r" -> "d"),
      Map("l" -> "b", "r" -> "a"),
      Map("l" -> "b", "r" -> "d"),
      Map("l" -> "d", "r" -> "a"),
      Map("l" -> "d", "r" -> "b")))
    result should use("TriadicSelection")
  }

  test("find friendly people") {
    // given
    execute( """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c"}), (d:Person{name:"d"}), (e:Person{name:"e"})
               |CREATE (a)-[:FRIEND]->(c), (b)-[:FRIEND]->(d), (c)-[:FRIEND]->(e), (d)-[:FRIEND]->(e)""".stripMargin)

    // when
    val result = profile(QUERY)

    // then
    result.toSet should equal(Set(
      Map("l" -> "a", "r" -> "e"),
      Map("l" -> "b", "r" -> "e"),
      Map("l" -> "d", "r" -> "c"),
      Map("l" -> "c", "r" -> "d"),
      Map("l" -> "e", "r" -> "a"),
      Map("l" -> "e", "r" -> "b")))
    result should use("TriadicSelection")
  }

  test("should not find my friends") {
    // given
    execute(
      """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c"})
        |CREATE (a)-[:FRIEND]->(b), (b)-[:FRIEND]->(c), (c)-[:FRIEND]->(a)""".stripMargin)

    // when
    val result: InternalExecutionResult = profile(QUERY)

    // then
    result should (use("TriadicSelection") and be(empty))
  }

  test("triadic should not handle complex incoming predicates for now") {
    // given
    execute("CREATE INDEX ON :Person(name)")
    execute( """CREATE (a:Person{name:"a"}), (b:Person{name:"b"}), (c:Person{name:"c",age:39}), (d:Person{name:"d"}), (e:Person{name:"e"})
               |CREATE (a)-[:FRIEND]->(b), (b)-[:FRIEND]->(c), (b)-[:FRIEND]->(d), (b)-[:FRIEND]->(e)
               |CREATE (a)-[:FRIEND]->(c), (a)-[:FRIEND]->(d), (c)-[:FRIEND]->(d)""".stripMargin)

    // when
    val queryWithPredicates = """
               |MATCH (a:Person)-[:FRIEND]->(b:Person)-[:FRIEND]->(c:Person)
               |USING INDEX a:Person(name)
               |WHERE a.name = 'a' AND b.age = 39 AND exists(c.name) AND (a)-[:FRIEND]->(c)
               |RETURN a.name AS l, b.name as m, c.name AS r""".stripMargin
    val result = profile(queryWithPredicates)

    // then
    result.toSet should equal(Set(Map("l" -> "a", "m" -> "c", "r" -> "d")))
    result shouldNot use("TriadicSelection")
  }

  test("Triadic should support StackOverflow example") {
    val create_contraints =
      """
        |CREATE CONSTRAINT ON (user:User) ASSERT user.neogen_id IS UNIQUE;
        |CREATE CONSTRAINT ON (p1:Post) ASSERT p1.neogen_id IS UNIQUE;
        |CREATE CONSTRAINT ON (p2:Post) ASSERT p2.neogen_id IS UNIQUE;
      """.stripMargin

    val create_model =
      """
        |MERGE (n1:User {neogen_id: '8aca6d28a0d2a9e1818bce6b69df6d3266a333bd' }) SET n1.firstname = 'You';
        |MERGE (n2:User {neogen_id: 'bcd9b073e2d86d6ae2471695bb5980e552fb22bc' }) SET n2.firstname = 'Me';
        |MERGE (n3:User {neogen_id: '7cc089d52179179c3f92c852b0f7ff60709edb57' }) SET n3.firstname = 'Someone';
        |MERGE (n4:Post {neogen_id: 'ae47ef9c49274c3ed8de6a71c41936f6f290f947' });
        |MERGE (n5:Post {neogen_id: '669a6c09b8b4846e48e2184a600d60030c0c43f0' });
        |MERGE (n6:Post {neogen_id: 'd55657f432e06cfa81ff46044e7763bea0017168' });
        |MERGE (n7:Post {neogen_id: 'a7e31ed01a6efa333885180c4ce09321ff36c9b3' });
        |MERGE (n8:Post {neogen_id: '147aa22fa236c464bdc075d0dc00fd41d9f25112' });
        |MERGE (n9:Post {neogen_id: '5e6479b37697a4bfc4a60b73c1f0c1cab2a90e55' });
        |MERGE (n10:Post {neogen_id: '8f8cb41236fb3ecc7000e55be4fc571ecfec2462' });
        |MERGE (n11:Post {neogen_id: '0cdd1458a98dd29912f52f73849ef6021a4a8e8b' });
        |MERGE (n12:Post {neogen_id: 'be251e3d0f83ac550ed904c345e027504fbeb354' });
        |MERGE (n13:Post {neogen_id: '65580e5c328d16c3248f77424135d2ec277a14e1' });
        |MERGE (n14:Post {neogen_id: 'b72543b17d170b65b42359db60cc9df71155fd65' });
        |MERGE (n15:Post {neogen_id: '5276d8ed757281704ec839089cdc62b508f76017' });
        |MERGE (n16:Post {neogen_id: 'c7cff71b9c7d2f5d298593fe76281815f3238242' });
        |MERGE (n17:Post {neogen_id: 'e1addbe6bd91eaaa7f8c6210786779eec9c12746' });
        |MERGE (n18:Post {neogen_id: '3df095dc1a863d198994329d15a1bd0cdde1dfcd' });
        |MATCH (s1:User {neogen_id: 'bcd9b073e2d86d6ae2471695bb5980e552fb22bc'}), (e1:Post { neogen_id: 'ae47ef9c49274c3ed8de6a71c41936f6f290f947'})
        |MERGE (s1)-[edge1:POSTED]->(e1);
        |MATCH (s2:User {neogen_id: 'bcd9b073e2d86d6ae2471695bb5980e552fb22bc'}), (e2:Post { neogen_id: '669a6c09b8b4846e48e2184a600d60030c0c43f0'})
        |MERGE (s2)-[edge2:POSTED]->(e2);
        |MATCH (s3:User {neogen_id: 'bcd9b073e2d86d6ae2471695bb5980e552fb22bc'}), (e3:Post { neogen_id: 'd55657f432e06cfa81ff46044e7763bea0017168'})
        |MERGE (s3)-[edge3:POSTED]->(e3);
        |MATCH (s4:User {neogen_id: '8aca6d28a0d2a9e1818bce6b69df6d3266a333bd'}), (e4:Post { neogen_id: 'a7e31ed01a6efa333885180c4ce09321ff36c9b3'})
        |MERGE (s4)-[edge4:POSTED]->(e4);
        |MATCH (s5:User {neogen_id: '7cc089d52179179c3f92c852b0f7ff60709edb57'}), (e5:Post { neogen_id: '147aa22fa236c464bdc075d0dc00fd41d9f25112'})
        |MERGE (s5)-[edge5:POSTED]->(e5);
        |MATCH (s6:User {neogen_id: '7cc089d52179179c3f92c852b0f7ff60709edb57'}), (e6:Post { neogen_id: '5e6479b37697a4bfc4a60b73c1f0c1cab2a90e55'})
        |MERGE (s6)-[edge6:POSTED]->(e6);
        |MATCH (s7:User {neogen_id: '7cc089d52179179c3f92c852b0f7ff60709edb57'}), (e7:Post { neogen_id: '8f8cb41236fb3ecc7000e55be4fc571ecfec2462'})
        |MERGE (s7)-[edge7:POSTED]->(e7);
        |MATCH (s8:User {neogen_id: '8aca6d28a0d2a9e1818bce6b69df6d3266a333bd'}), (e8:Post { neogen_id: '0cdd1458a98dd29912f52f73849ef6021a4a8e8b'})
        |MERGE (s8)-[edge8:POSTED]->(e8);
        |MATCH (s9:User {neogen_id: 'bcd9b073e2d86d6ae2471695bb5980e552fb22bc'}), (e9:Post { neogen_id: 'be251e3d0f83ac550ed904c345e027504fbeb354'})
        |MERGE (s9)-[edge9:POSTED]->(e9);
        |MATCH (s10:User {neogen_id: '8aca6d28a0d2a9e1818bce6b69df6d3266a333bd'}), (e10:Post { neogen_id: '65580e5c328d16c3248f77424135d2ec277a14e1'})
        |MERGE (s10)-[edge10:POSTED]->(e10);
        |MATCH (s11:User {neogen_id: '7cc089d52179179c3f92c852b0f7ff60709edb57'}), (e11:Post { neogen_id: 'b72543b17d170b65b42359db60cc9df71155fd65'})
        |MERGE (s11)-[edge11:POSTED]->(e11);
        |MATCH (s12:User {neogen_id: '7cc089d52179179c3f92c852b0f7ff60709edb57'}), (e12:Post { neogen_id: '5276d8ed757281704ec839089cdc62b508f76017'})
        |MERGE (s12)-[edge12:POSTED]->(e12);
        |MATCH (s13:User {neogen_id: 'bcd9b073e2d86d6ae2471695bb5980e552fb22bc'}), (e13:Post { neogen_id: 'c7cff71b9c7d2f5d298593fe76281815f3238242'})
        |MERGE (s13)-[edge13:POSTED]->(e13);
        |MATCH (s14:User {neogen_id: '8aca6d28a0d2a9e1818bce6b69df6d3266a333bd'}), (e14:Post { neogen_id: 'e1addbe6bd91eaaa7f8c6210786779eec9c12746'})
        |MERGE (s14)-[edge14:POSTED]->(e14);
        |MATCH (s15:User {neogen_id: '8aca6d28a0d2a9e1818bce6b69df6d3266a333bd'}), (e15:Post { neogen_id: '3df095dc1a863d198994329d15a1bd0cdde1dfcd'})
        |MERGE (s15)-[edge15:POSTED]->(e15);
        |MATCH (s16:Post {neogen_id: 'ae47ef9c49274c3ed8de6a71c41936f6f290f947'}), (e16:Post { neogen_id: '5e6479b37697a4bfc4a60b73c1f0c1cab2a90e55'})
        |MERGE (s16)-[edge16:ANSWER]->(e16);
        |MATCH (s17:Post {neogen_id: 'ae47ef9c49274c3ed8de6a71c41936f6f290f947'}), (e17:Post { neogen_id: '8f8cb41236fb3ecc7000e55be4fc571ecfec2462'})
        |MERGE (s17)-[edge17:ANSWER]->(e17);
        |MATCH (s18:Post {neogen_id: '669a6c09b8b4846e48e2184a600d60030c0c43f0'}), (e18:Post { neogen_id: '0cdd1458a98dd29912f52f73849ef6021a4a8e8b'})
        |MERGE (s18)-[edge18:ANSWER]->(e18);
        |MATCH (s19:Post {neogen_id: 'ae47ef9c49274c3ed8de6a71c41936f6f290f947'}), (e19:Post { neogen_id: 'be251e3d0f83ac550ed904c345e027504fbeb354'})
        |MERGE (s19)-[edge19:ANSWER]->(e19);
        |MATCH (s20:Post {neogen_id: '669a6c09b8b4846e48e2184a600d60030c0c43f0'}), (e20:Post { neogen_id: '65580e5c328d16c3248f77424135d2ec277a14e1'})
        |MERGE (s20)-[edge20:ANSWER]->(e20);
        |MATCH (s21:Post {neogen_id: 'a7e31ed01a6efa333885180c4ce09321ff36c9b3'}), (e21:Post { neogen_id: 'b72543b17d170b65b42359db60cc9df71155fd65'})
        |MERGE (s21)-[edge21:ANSWER]->(e21);
        |MATCH (s22:Post {neogen_id: 'a7e31ed01a6efa333885180c4ce09321ff36c9b3'}), (e22:Post { neogen_id: '5276d8ed757281704ec839089cdc62b508f76017'})
        |MERGE (s22)-[edge22:ANSWER]->(e22);
        |MATCH (s23:Post {neogen_id: 'ae47ef9c49274c3ed8de6a71c41936f6f290f947'}), (e23:Post { neogen_id: 'c7cff71b9c7d2f5d298593fe76281815f3238242'})
        |MERGE (s23)-[edge23:ANSWER]->(e23);
        |MATCH (s24:Post {neogen_id: 'a7e31ed01a6efa333885180c4ce09321ff36c9b3'}), (e24:Post { neogen_id: 'e1addbe6bd91eaaa7f8c6210786779eec9c12746'})
        |MERGE (s24)-[edge24:ANSWER]->(e24);
        |MATCH (s25:Post {neogen_id: 'd55657f432e06cfa81ff46044e7763bea0017168'}), (e25:Post { neogen_id: '3df095dc1a863d198994329d15a1bd0cdde1dfcd'})
        |MERGE (s25)-[edge25:ANSWER]->(e25);
        |MATCH (n1:User) REMOVE n1.neogen_id;
        |MATCH (n2:Post) REMOVE n2.neogen_id;
        |MATCH (n3:Post) REMOVE n3.neogen_id;
      """.stripMargin

    Seq(create_contraints, create_model) foreach { queries =>
      queries.split(";").collect {
        case q if q.trim.nonEmpty => q.trim
      }.foreach { query =>
        execute(query)
      }
    }

    Map(
      "non-triadic1" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q:Post)-[:ANSWER]->(a:Post)<-[:POSTED]-(u) RETURN u, a",
        "command" -> "Expand(Into)",
        "count" -> 3),
      "non-triadic2" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q)-[:ANSWER]->(a)<-[:POSTED]-(u) RETURN u, a",
        "command" -> "Expand(Into)",
        "count" -> 3),
      "triadic-neg1" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q:Post)-[:ANSWER]->(a:Post) WHERE NOT (u)-[:POSTED]->(a) RETURN u, a",
        "command" -> "TriadicSelection",
        "count" -> 7),
      "triadic-neg2" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q)-[:ANSWER]->(a) WHERE NOT (u)-[:POSTED]->(a) RETURN u, a",
        "command" -> "TriadicSelection",
        "count" -> 7),
      "triadic-neg3" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q)-[:ANSWER]->(a:Post) WHERE NOT (u)-[:POSTED]->(a) RETURN u, a",
        "command" -> "TriadicSelection",
        "count" -> 7),
      "triadic-neg4" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q:Post)-[:ANSWER]->(a) WHERE NOT (u)-[:POSTED]->(a) RETURN u, a",
        "command" -> "AntiSemiApply",
        "count" -> 7),
      "triadic-pos1" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q:Post)-[:ANSWER]->(a:Post) WHERE (u)-[:POSTED]->(a) RETURN u, a",
        "command" -> "TriadicSelection",
        "count" -> 3),
      "triadic-pos2" -> Map(
        "query" -> "MATCH (u:User)-[:POSTED]->(q)-[:ANSWER]->(a) WHERE (u)-[:POSTED]->(a) RETURN u, a",
        "command" -> "TriadicSelection",
        "count" -> 3)
    ).collect {
      case (name, config:Map[String,Any]) =>
        val query = config("query").asInstanceOf[String]
        val count = config("count").asInstanceOf[Int]
        val command = config("command").asInstanceOf[String]
        val result = execute(query)
        result should haveCount(count)
        result should use(command)
    }
  }
}
