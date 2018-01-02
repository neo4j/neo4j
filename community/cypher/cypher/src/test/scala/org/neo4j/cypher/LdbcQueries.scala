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
package org.neo4j.cypher

import org.joda.time.{DateTimeZone, DateTime}

/**
 * These are the 14 LDBC queries that runs in the LDBC projects. The queries are (semi-)generated so the idea is rather
 * to have them easy to generate than easy to read so refactor with caution.
 */
object LdbcQueries {

  sealed trait LdbcQuery {

    def name: String

    def createQuery: String

    def createParams: Map[String, Any]

    def query: String

    def params: Map[String, Any]

    def expectedResult: List[Map[String, Any]]

    val constraintQueries: Seq[String] = Seq(
      "CREATE CONSTRAINT ON (node:Person) ASSERT node.id IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Country) ASSERT node.id IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Country) ASSERT node.name IS UNIQUE",
      "CREATE CONSTRAINT ON (node:City) ASSERT node.name IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Tag) ASSERT node.id IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Tag) ASSERT node.name IS UNIQUE",
      "CREATE CONSTRAINT ON (node:TagClass) ASSERT node.name IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Comment) ASSERT node.id IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Post) ASSERT node.id IS UNIQUE",
      "CREATE CONSTRAINT ON (node:Forum) ASSERT node.id IS UNIQUE",
      "CREATE INDEX ON :Person(firstName)",
      "CREATE INDEX ON :Person(lastName)",
      "CREATE INDEX ON :Person(birthday_month)",
      "CREATE INDEX ON :Person(birthday_day)"
    )
  }

  object Query1 extends LdbcQuery {

    val name = "LDBC Query 1"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person0:Person {person0}),
                        | (f1:Person {f1}),
                        | (f2:Person {f2}),
                        | (f3:Person {f3}),
                        | (ff11:Person {ff11}),
                        | (fff111:Person {fff111}),
                        | (ffff1111:Person {ffff1111}),
                        | (fffff11111:Person {fffff11111}),
                        | (ff21:Person {ff21}),
                        | (fff211:Person {fff211}),
                        | (ff31:Person {ff31}),
                        | (uni0:University {uni0}),
                        | (uni1:University {uni1}),
                        | (uni2:University {uni2}),
                        | (company0:Company {company0}),
                        | (company1:Company {company1}),
                        | (city0:City {city0}),
                        | (city1:City {city1}),
                        | (country0:Country {country0}),
                        | (country1:Country {country1}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        | (person0)-[:KNOWS]->(f1),
                        | (person0)-[:KNOWS]->(f2),
                        | (person0)-[:KNOWS]->(f3),
                        | (f1)-[:KNOWS]->(ff11),
                        | (f2)-[:KNOWS]->(ff11),
                        | (f2)-[:KNOWS]->(ff21),
                        | (f3)-[:KNOWS]->(ff31),
                        | (ff11)-[:KNOWS]->(fff111),
                        | (fff111)-[:KNOWS]->(ffff1111),
                        | (ffff1111)-[:KNOWS]->(fffff11111),
                        | (ff21)-[:KNOWS]->(fff211),
                        | (f3)-[:KNOWS]->(f2),
                        | (person0)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f1)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff11)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (fff111)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ffff1111)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (fffff11111)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f2)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (ff21)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (fff211)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (f3)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (ff31)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (city0)-[:IS_PART_OF]->(country0),
                        | (city1)-[:IS_PART_OF]->(country1),
                        | (company0)-[:ORGANISATION_IS_LOCATED_IN ]->(country0),
                        | (company1)-[:ORGANISATION_IS_LOCATED_IN ]->(country1),
                        | (uni0)-[:ORGANISATION_IS_LOCATED_IN ]->(city1),
                        | (uni1)-[:ORGANISATION_IS_LOCATED_IN ]->(city0),
                        | (uni2)-[:ORGANISATION_IS_LOCATED_IN ]->(city0),
                        | (f1)-[:STUDY_AT {f1StudyAtUni0}]->(uni0),
                        | (ff11)-[:STUDY_AT {ff11StudyAtUni1}]->(uni1),
                        | (ff11)-[:STUDY_AT {ff11StudyAtUni2}]->(uni2),
                        | (f2)-[:STUDY_AT {f2StudyAtUni2}]->(uni2),
                        | (f1)-[:WORKS_AT {f1WorkAtCompany0}]->(company0),
                        | (f3)-[:WORKS_AT {f3WorkAtCompany0}]->(company0),
                        | (ff21)-[:WORKS_AT {ff21WorkAtCompany1}]->(company1)""".stripMargin


    val query = """MATCH (:Person {id:{1}})-[path:KNOWS*1..3]-(friend:Person)
                  |WHERE friend.firstName = {2}
                  |WITH friend, min(length(path)) AS distance
                  |ORDER BY distance ASC, friend.lastName ASC, friend.id ASC
                  |LIMIT {3}
                  |MATCH (friend)-[:PERSON_IS_LOCATED_IN]->(friendCity:City)
                  |OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:ORGANISATION_IS_LOCATED_IN]->(uniCity:City)
                  |WITH friend, collect(CASE uni.name WHEN null THEN null ELSE [uni.name, studyAt.classYear, uniCity.name] END) AS unis, friendCity, distance
                  |OPTIONAL MATCH (friend)-[worksAt:WORKS_AT]->(company:Company)-[:ORGANISATION_IS_LOCATED_IN]->(companyCountry:Country)
                  |WITH friend, collect(CASE company.name WHEN null THEN null ELSE [company.name, worksAt.workFrom, companyCountry.name] END) AS companies, unis, friendCity, distance
                  |RETURN friend.id AS id, friend.lastName AS lastName, distance, friend.birthday AS birthday, friend.creationDate AS creationDate, friend.gender AS gender, friend.browserUsed AS browser, friend.locationIP AS locationIp, friend.email AS emails, friend.languages AS languages, friendCity.name AS cityName, unis, companies
                  |ORDER BY distance ASC, friend.lastName ASC, friend.id ASC
                  |LIMIT {3}""".stripMargin

    def createParams = Map("ff21" ->
      Map("id" -> 21, "languages" -> Seq.empty, "birthday" -> 21, "creationDate" -> 21, "lastName" -> "last21-ᚠさ丵פش", "browserUsed" -> "browser21", "email" -> Seq.empty, "locationIP" -> "ip21", "gender" -> "gender21", "firstName" -> "name1"), "company1" ->
      Map("id" -> 11, "name" -> "company1"), "city0" ->
      Map("id" -> 0, "name" -> "city0"), "city1" ->
      Map("id" -> 1, "name" -> "city1"), "ff11StudyAtUni2" ->
      Map("classYear" -> 2), "ff11StudyAtUni1" ->
      Map("classYear" -> 1), "company0" ->
      Map("id" -> 10, "name" -> "company0"), "fffff11111" ->
      Map("id" -> 11111, "languages" -> Seq("fffff11111language0"), "birthday" -> 11111, "creationDate" -> 11111, "lastName" -> "last11111-ᚠさ丵פش", "browserUsed" -> "browser11111", "email" -> Seq("fffff11111email1"), "locationIP" -> "ip11111", "gender" -> "gender11111", "firstName" -> "name0"), "f1StudyAtUni0" ->
      Map("classYear" -> 0), "f1" ->
      Map("id" -> 1, "languages" -> Seq("friend1language0"), "birthday" -> 1, "creationDate" -> 1, "lastName" -> "last1-ᚠさ丵פش", "browserUsed" -> "browser1", "email" -> Seq("friend1email1", "friend1email2"), "locationIP" -> "ip1", "gender" -> "gender1", "firstName" -> "name0"), "f3" ->
      Map("id" -> 3, "languages" -> Seq("friend3language0"), "birthday" -> 3, "creationDate" -> 3, "lastName" -> "last0-ᚠさ丵פش", "browserUsed" -> "browser3", "email" -> Seq("friend3email1", "friend3email2"), "locationIP" -> "ip3", "gender" -> "gender3", "firstName" -> "name0"), "ff11" ->
      Map("id" -> 11, "languages" -> Seq.empty, "birthday" -> 11, "creationDate" -> 11, "lastName" -> "last11-ᚠさ丵פش", "browserUsed" -> "browser11", "email" -> Seq.empty, "locationIP" -> "ip11", "gender" -> "gender11", "firstName" -> "name0"), "f2" ->
      Map("id" -> 2, "languages" -> Seq("friend2language0", "friend2language1"), "birthday" -> 2, "creationDate" -> 2, "lastName" -> "last0-ᚠさ丵פش", "browserUsed" -> "browser2", "email" -> Seq.empty, "locationIP" -> "ip2", "gender" -> "gender2", "firstName" -> "name0"), "ff21WorkAtCompany1" ->
      Map("workFrom" -> 2), "country0" ->
      Map("id" -> 10, "name" -> "country0"), "country1" ->
      Map("id" -> 11, "name" -> "country1"), "fff111" ->
      Map("id" -> 111, "languages" -> Seq("fff111language0", "fff111language1", "fff111language2"), "birthday" -> 111, "creationDate" -> 111, "lastName" -> "last111-ᚠさ丵פش", "browserUsed" -> "browser111", "email" -> Seq("fff111email1", "fff111email2", "fff111email3"), "locationIP" -> "ip111", "gender" -> "gender111", "firstName" -> "name1"), "person0" ->
      Map("id" -> 0, "languages" -> Seq("person0language0", "person0language1"), "birthday" -> 0, "creationDate" -> 0, "lastName" -> "zero-ᚠさ丵פش", "browserUsed" -> "browser0", "email" -> Seq("person0email1", "person0email2"), "locationIP" -> "ip0", "gender" -> "gender0", "firstName" -> "person"), "fff211" ->
      Map("id" -> 211, "languages" -> Seq.empty, "birthday" -> 211, "creationDate" -> 211, "lastName" -> "last211-ᚠさ丵פش", "browserUsed" -> "browser211", "email" -> Seq("fff211email1"), "locationIP" -> "ip211", "gender" -> "gender211", "firstName" -> "name1"), "ffff1111" ->
      Map("id" -> 1111, "languages" -> Seq("ffff1111language0"), "birthday" -> 1111, "creationDate" -> 1111, "lastName" -> "last1111-ᚠさ丵פش", "browserUsed" -> "browser1111", "email" -> Seq("ffff1111email1"), "locationIP" -> "ip1111", "gender" -> "gender1111", "firstName" -> "name0"), "f3WorkAtCompany0" ->
      Map("workFrom" -> 1), "f1WorkAtCompany0" ->
      Map("workFrom" -> 0), "uni0" ->
      Map("id" -> 0, "name" -> "uni0"), "uni1" ->
      Map("id" -> 1, "name" -> "uni1"), "f2StudyAtUni2" ->
      Map("classYear" -> 3), "uni2" ->
      Map("id" -> 2, "name" -> "uni2"), "ff31" ->
      Map("id" -> 31, "languages" -> Seq.empty, "birthday" -> 31, "creationDate" -> 31, "lastName" -> "last31-ᚠさ丵פش", "browserUsed" -> "browser31", "email" -> Seq.empty, "locationIP" -> "ip31", "gender" -> "gender31", "firstName" -> "name0"))

    def params = Map("1" -> 0, "2" -> "name0", "3" -> 6)

    def expectedResult: List[Map[String, Any]] = {
      List(
        Map("creationDate" -> 2, "gender" -> "gender2", "distance" -> 1, "unis" -> List(List[Any]("uni2", 3, "city0")), "locationIp" -> "ip2",
          "languages" -> List("friend2language0", "friend2language1"), "birthday" -> 2, "cityName" -> "city1",
          "lastName" -> "last0-ᚠさ丵פش", "id" -> 2, "emails" -> Seq.empty, "browser" -> "browser2", "companies" -> Seq.empty),
        Map("creationDate" -> 3, "gender" -> "gender3", "distance" -> 1, "unis" -> Seq.empty, "locationIp" -> "ip3",
          "languages" -> List("friend3language0"), "birthday" -> 3, "cityName" -> "city1", "lastName" -> "last0-ᚠさ丵פش",
          "id" -> 3, "emails" -> List("friend3email1", "friend3email2"), "browser" -> "browser3",
          "companies" -> List(List[Any]("company0", 1, "country0"))),
        Map("creationDate" -> 1, "gender" -> "gender1", "distance" -> 1,
          "unis" -> List(List[Any]("uni0", 0, "city1")), "locationIp" -> "ip1", "languages" -> List("friend1language0"),
          "birthday" -> 1, "cityName" -> "city0", "lastName" -> "last1-ᚠさ丵פش", "id" -> 1,
          "emails" -> List("friend1email1", "friend1email2"), "browser" -> "browser1", "companies" -> List(List[Any]("company0", 0, "country0"))),
        Map("creationDate" -> 11, "gender" -> "gender11", "distance" -> 2, "unis" -> List(List[Any]("uni2", 2, "city0"),
          List[Any]("uni1", 1, "city0")), "locationIp" -> "ip11", "languages" -> Seq.empty, "birthday" -> 11, "cityName" -> "city0",
          "lastName" -> "last11-ᚠさ丵פش", "id" -> 11, "emails" -> Seq.empty, "browser" -> "browser11", "companies" -> Seq.empty),
        Map("creationDate" -> 31, "gender" -> "gender31", "distance" -> 2, "unis" -> Seq.empty, "locationIp" -> "ip31",
          "languages" -> Seq.empty, "birthday" -> 31, "cityName" -> "city1", "lastName" -> "last31-ᚠさ丵פش", "id" -> 31,
          "emails" -> Seq.empty, "browser" -> "browser31", "companies" -> Seq.empty)
      )
    }
  }

  object Query2 extends LdbcQuery {

    val name = "LDBC Query 2"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (country0:Country {country0}),
                        | (forum1:Forum {forum1}), (p1:Person {p1}), (f2:Person {f2}), (f3:Person {f3}), (f4:Person {f4}),
                        | (s5:Person {s5}), (ff6:Person {ff6}),(s7:Person {s7}),
                        | (f3Post1:Post {f3Post1}), (f3Post2:Post {f3Post2}), (f3Post3:Post {f3Post3}),
                        | (f4Post1:Post {f4Post1}), (f2Post1:Post {f2Post1}), (f2Post2:Post {f2Post2}), (f2Post3:Post {f2Post3}),
                        | (s5Post1:Post {s5Post1}), (s5Post2:Post {s5Post2}), (ff6Post1:Post {ff6Post1}),
                        | (s7Post1:Post {s7Post1}), (s7Post2:Post {s7Post2}),
                        | (f2Comment1:Comment {f2Comment1}), (f2Comment2:Comment {f2Comment2}),
                        | (s5Comment1:Comment {s5Comment1}), (f3Comment1:Comment {f3Comment1}), (p1Comment1:Comment {p1Comment1})
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |FOREACH (n IN [f3, f2, f4] | CREATE (p1)-[:KNOWS]->(n) )
                        |FOREACH (n IN [ff6] | CREATE (f2)-[:KNOWS]->(n) )
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [f4Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(f4) )
                        |FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s7) )
                        |FOREACH (n IN [ff6Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(ff6) )
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3, f4Post1, f2Post1, f2Post2, f2Post3, s5Post1, s5Post2, s7Post1, s7Post2, ff6Post1]| CREATE (forum1)-[:CONTAINER_OF]->(n) )
                        |FOREACH (n IN [f2Comment1, f2Comment2] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [p1Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(p1) )
                        |FOREACH (n IN [f3Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [s5Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [f2Comment1, s5Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f3Post2) )
                        |FOREACH (n IN [f2Comment2] | CREATE (n)-[:REPLY_OF_COMMENT]->(s5Comment1) )
                        |FOREACH (n IN [f3Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f4Post1) )
                        |FOREACH (n IN [p1Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f2Post2) )
                      """.stripMargin

    def createParams =
      Map("s5Post1" ->
        Map("content" -> "[s5Post1] content", "id" -> 8, "creationDate" -> 1, "browserUsed" -> "5", "locationIP" -> "5", "imageFile" -> "[s5Post1] image", "language" -> "5"), "p1" ->
        Map("id" -> 1, "languages" -> Seq("1a", "1b"), "birthday" -> 1, "creationDate" -> 1, "lastName" -> "last1-ᚠさ丵פش", "browserUsed" -> "1", "email" -> Seq("person1@email1", "person1@email2"), "locationIP" -> "1", "gender" -> "1", "firstName" -> "person1"), "f2Comment1" ->
        Map("content" -> "[f2Comment1] content", "id" -> 13, "creationDate" -> 2, "browserUsed" -> "2", "locationIP" -> "2"), "f2Comment2" ->
        Map("content" -> "[f2Comment2] content", "id" -> 14, "creationDate" -> 4, "browserUsed" -> "2", "locationIP" -> "2"), "ff6" ->
        Map("id" -> 6, "languages" -> Seq("6a", "6b"), "birthday" -> 6, "creationDate" -> 6, "lastName" -> "last6-ᚠさ丵פش", "browserUsed" -> "6", "email" -> Seq("ff6@email1"), "locationIP" -> "6", "gender" -> "6", "firstName" -> "ff6"), "s5Comment1" ->
        Map("content" -> "[s5Comment1] content", "id" -> 15, "creationDate" -> 1, "browserUsed" -> "5", "locationIP" -> "5"), "f3Comment1" ->
        Map("content" -> "[f3Comment1] content", "id" -> 16, "creationDate" -> 3, "browserUsed" -> "3", "locationIP" -> "3"), "s7" ->
        Map("id" -> 7, "languages" -> Seq("7"), "birthday" -> 7, "creationDate" -> 7, "lastName" -> "last7-ᚠさ丵פش", "browserUsed" -> "7", "email" -> Seq("s7@email1"), "locationIP" -> "7", "gender" -> "7", "firstName" -> "s7"), "s5Post2" ->
        Map("content" -> "[s5Post2] content", "id" -> 9, "creationDate" -> 1, "browserUsed" -> "5", "locationIP" -> "5", "imageFile" -> "[s5Post2] image", "language" -> "5"), "s5" ->
        Map("id" -> 5, "languages" -> Seq("5"), "birthday" -> 5, "creationDate" -> 5, "lastName" -> "last5-ᚠさ丵פش", "browserUsed" -> "5", "email" -> Seq("stranger5@email1"), "locationIP" -> "5", "gender" -> "5", "firstName" -> "s5"), "f3" ->
        Map("id" -> 3, "languages" -> Seq("3a", "3b"), "birthday" -> 3, "creationDate" -> 3, "lastName" -> "last3-ᚠさ丵פش", "browserUsed" -> "3", "email" -> Seq("friend3@email1", "friend3@email2"), "locationIP" -> "3", "gender" -> "3", "firstName" -> "f3"), "f2Post3" ->
        Map("content" -> "[f2Post3] content", "id" -> 7, "creationDate" -> 2, "browserUsed" -> "safari", "locationIP" -> "31.55.91.343", "imageFile" -> "[f2Post3] image", "language" -> "2"), "s7Post1" ->
        Map("content" -> "[s7Post1] content", "id" -> 10, "creationDate" -> 1, "browserUsed" -> "7", "locationIP" -> "7", "imageFile" -> "[s7Post1] image", "language" -> "7a"), "f2" ->
        Map("id" -> 2, "languages" -> Seq("2"), "birthday" -> 2, "creationDate" -> 2, "lastName" -> "last2-ᚠさ丵פش", "browserUsed" -> "2", "email" -> Seq("friend2@email1"), "locationIP" -> "2", "gender" -> "2", "firstName" -> "f2"), "f2Post2" ->
        Map("content" -> "[f2Post2] content", "id" -> 6, "creationDate" -> 2, "browserUsed" -> "2", "locationIP" -> "2", "imageFile" -> "[f2Post2] image", "language" -> "2"), "f2Post1" ->
        Map("content" -> "[f2Post1] content", "id" -> 5, "creationDate" -> 4, "browserUsed" -> "2", "locationIP" -> "2", "imageFile" -> "[f2Post1] image", "language" -> "2"), "f4" ->
        Map("id" -> 4, "languages" -> Seq("4a", "4b"), "birthday" -> 4, "creationDate" -> 4, "lastName" -> "last4-ᚠさ丵פش", "browserUsed" -> "4", "email" -> Seq("friend4@email1"), "locationIP" -> "4", "gender" -> "4", "firstName" -> "f4"), "s7Post2" ->
        Map("content" -> "[s7Post2] content", "id" -> 11, "creationDate" -> 1, "browserUsed" -> "7", "locationIP" -> "7", "imageFile" -> "[s7Post2] image", "language" -> "7"), "country0" ->
        Map("id" -> 10, "name" -> "country0"), "forum1" ->
        Map("id" -> 1, "title" -> "forum1-ᚠさ丵פش"), "ff6Post1" ->
        Map("content" -> "[ff6Post1] content", "id" -> 12, "creationDate" -> 1, "browserUsed" -> "6", "locationIP" -> "6", "imageFile" -> "[ff6Post1] image", "language" -> "6"), "f3Post1" ->
        Map("content" -> "[f3Post1] content", "id" -> 1, "creationDate" -> 4, "browserUsed" -> "3", "locationIP" -> "3", "imageFile" -> "3", "language" -> "3"), "f3Post2" ->
        Map("content" -> "[f3Post2] content", "id" -> 2, "creationDate" -> 3, "browserUsed" -> "3", "locationIP" -> "3", "imageFile" -> "[f3Post2] image", "language" -> "3"), "f3Post3" ->
        Map("id" -> 3, "creationDate" -> 3, "browserUsed" -> "3", "locationIP" -> "3", "imageFile" -> "[f3Post3] image", "language" -> "3"), "p1Comment1" ->
        Map("content" -> "[p1Comment1] content", "id" -> 17, "creationDate" -> 1, "browserUsed" -> "browser1", "locationIP" -> "1"), "f4Post1" ->
        Map("content" -> "[f4Post1] content", "id" -> 4, "creationDate" -> 4, "browserUsed" -> "4", "locationIP" -> "4", "imageFile" -> "[f4Post1] image", "language" -> "4"))

    val query = """MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
                  |WHERE message.creationDate <= {2} AND (message:Post OR message:Comment)
                  |RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, message.id AS messageId, CASE has(message.content) WHEN true THEN message.content ELSE message.imageFile END AS messageContent,
                  | message.creationDate AS messageDate
                  |ORDER BY messageDate DESC, messageId ASC
                  |LIMIT {3}""".stripMargin


    def params = Map("1" -> 1, "2" -> 3, "3" -> 10)

    def expectedResult = List(
      Map("personId" -> 3, "messageId" -> 2, "personLastName" -> "last3-ᚠさ丵פش",
        "messageContent" -> "[f3Post2] content", "messageDate" -> 3, "personFirstName" -> "f3"),
      Map("personId" -> 3, "messageId" -> 3, "personLastName" -> "last3-ᚠさ丵פش",
        "messageContent" -> "[f3Post3] image", "messageDate" -> 3, "personFirstName" -> "f3"),
      Map("personId" -> 3, "messageId" -> 16, "personLastName" -> "last3-ᚠさ丵פش",
        "messageContent" -> "[f3Comment1] content", "messageDate" -> 3, "personFirstName" -> "f3"),
      Map("personId" -> 2, "messageId" -> 6, "personLastName" -> "last2-ᚠさ丵פش",
        "messageContent" -> "[f2Post2] content", "messageDate" -> 2, "personFirstName" -> "f2"),
      Map("personId" -> 2, "messageId" -> 7, "personLastName" -> "last2-ᚠさ丵פش",
        "messageContent" -> "[f2Post3] content", "messageDate" -> 2, "personFirstName" -> "f2"),
      Map("personId" -> 2, "messageId" -> 13, "personLastName" -> "last2-ᚠさ丵פش",
        "messageContent" -> "[f2Comment1] content", "messageDate" -> 2, "personFirstName" -> "f2"))
  }

  object Query3 extends LdbcQuery {

    val name = "LDBC Query 3"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person1:Person {person1}), (f2:Person {f2}), (f3:Person {f3}), (f4:Person {f4}),
                        | (s5:Person {s5}), (ff6:Person {ff6}),(s7:Person {s7}),
                        | (city1:City {city1}),  (city2:City {city2}), (city3:City {city3}),
                        | (city4:City {city4}), (city5:City {city5}),
                        | (country1:Country {country1}), (country2:Country {country2}),
                        | (country3:Country {country3}), (country4:Country {country4}),
                        | (country5:Country {country5}),
                        | (f3Post1:Post {f3Post1}),
                        |(f3Post2:Post {f3Post2}),
                        | (f3Post3:Post {f3Post3}),
                        | (f4Post1:Post {f4Post1}),
                        |(f2Post1:Post {f2Post1}),
                        | (f2Post2:Post {f2Post2}),
                        |(f2Post3:Post {f2Post3}),
                        | (s5Post1:Post {s5Post1}),
                        | (s5Post2:Post {s5Post2}),
                        | (ff6Post1:Post {ff6Post1}),
                        | (s7Post1:Post {s7Post1}),
                        | (s7Post2:Post {s7Post2}),
                        | (f2Comment1:Comment {f2Comment1}), (f2Comment2:Comment {f2Comment2}),
                        | (s5Comment1:Comment {s5Comment1}), (f3Comment1:Comment {f3Comment1}), (person1Comment1:Comment {person1Comment1}),
                        | (ff6Comment1:Comment {ff6Comment1}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        | (city2)-[:IS_PART_OF]->(country1), (city1)-[:IS_PART_OF]->(country2),
                        | (city3)-[:IS_PART_OF]->(country3), (city4)-[:IS_PART_OF]->(country5), (city5)-[:IS_PART_OF]->(country4),
                        | (ff6)-[:PERSON_IS_LOCATED_IN]->(city4),
                        | (person1)-[:PERSON_IS_LOCATED_IN]->(city2),
                        | (f2)-[:PERSON_IS_LOCATED_IN]->(city4),
                        | (f3)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (f4)-[:PERSON_IS_LOCATED_IN]->(city3),
                        | (s5)-[:PERSON_IS_LOCATED_IN]->(city2),
                        | (s7)-[:PERSON_IS_LOCATED_IN]->(city2),
                        | (s5Comment1)-[:COMMENT_IS_LOCATED_IN]->(country1),
                        | (f2Comment2)-[:COMMENT_IS_LOCATED_IN]->(country4),
                        | (f3Comment1)-[:COMMENT_IS_LOCATED_IN]->(country3),
                        | (person1Comment1)-[:COMMENT_IS_LOCATED_IN]->(country2),
                        | (f2Comment1)-[:COMMENT_IS_LOCATED_IN]->(country1),
                        | (ff6Comment1)-[:COMMENT_IS_LOCATED_IN]->(country2)
                        |FOREACH (n IN [f3Post1,f2Post2, f2Post3] | CREATE (n)-[:POST_IS_LOCATED_IN]->(country2) )
                        |FOREACH (n IN [ff6Post1,f3Post2,f3Post3,f2Post1,s7Post1,s7Post2,s5Post2] | CREATE (n)-[:POST_IS_LOCATED_IN]->(country1) )
                        |FOREACH (n IN [f4Post1] | CREATE (n)-[:POST_IS_LOCATED_IN]->(country3) )
                        |FOREACH (n IN [s5Post1] | CREATE (n)-[:POST_IS_LOCATED_IN]->(country4) )
                        |FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:KNOWS]->(n) )
                        |FOREACH (n IN [ff6] | CREATE (f2)-[:KNOWS]->(n) )
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [f4Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(f4) )
                        |FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s7) )
                        |FOREACH (n IN [ff6Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(ff6) )
                        |FOREACH (n IN [f2Comment1, f2Comment2] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [person1Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(person1) )
                        |FOREACH (n IN [f3Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [s5Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [ff6Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(ff6) )
                        |FOREACH (n IN [f2Comment1, s5Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f3Post2) )
                        |FOREACH (n IN [f2Comment2] | CREATE (n)-[:REPLY_OF_COMMENT]->(s5Comment1) )
                        |FOREACH (n IN [f3Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f4Post1) )
                        |FOREACH (n IN [person1Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f2Post2) )
                        |FOREACH (n IN [ff6Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f2Post3) )""".stripMargin

    def createParams =
      Map("city3" ->
        Map("id" -> 3, "name" -> "city3"), "s5Post1" ->
        Map("content" -> "[s5Post1] content", "id" -> 8, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "city4" ->
        Map("id" -> 4, "name" -> "city4"), "city5" ->
        Map("id" -> 5, "name" -> "city5"), "f2Comment1" ->
        Map("content" -> "[f2Comment1] content", "id" -> 13, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2"), "f2Comment2" ->
        Map("content" -> "[f2Comment2] content", "id" -> 14, "creationDate" -> 946940400000L, "browserUsed" -> "browser2", "locationIP" -> "ip2"), "person1Comment1" ->
        Map("content" -> "[person1Comment1] content", "id" -> 17, "creationDate" -> 946854000000L, "browserUsed" -> "browser1", "locationIP" -> "ip1"), "city1" ->
        Map("id" -> 1, "name" -> "city1"), "city2" ->
        Map("id" -> 2, "name" -> "city2"), "ff6Comment1" ->
        Map("content" -> "[ff6Comment1] content", "id" -> 18, "creationDate" -> 946940400000L, "browserUsed" -> "browser6", "locationIP" -> "ip6"), "ff6" ->
        Map("id" -> 6, "languages" -> Seq("language6a", "language6b"), "birthday" -> 6, "creationDate" -> 6, "lastName" -> "last6-ᚠさ丵פش", "browserUsed" -> "browser6", "email" -> Seq("ff6@email.com"), "locationIP" -> "ip6", "gender" -> "gender6", "firstName" -> "ff6"), "s5Comment1" ->
        Map("content" -> "[s5Comment1] content", "id" -> 15, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5"), "f3Comment1" ->
        Map("content" -> "[f3Comment1] content", "id" -> 16, "creationDate" -> 946681200000L, "browserUsed" -> "browser3", "locationIP" -> "ip3"), "s7" ->
        Map("id" -> 7, "languages" -> Seq("language7"), "birthday" -> 7, "creationDate" -> 7, "lastName" -> "last7-ᚠさ丵פش", "browserUsed" -> "browser7", "email" -> Seq("s7@email.com"), "locationIP" -> "ip7", "gender" -> "gender7", "firstName" -> "s7"), "s5Post2" ->
        Map("content" -> "[s5Post2] content", "id" -> 9, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "s7Post1" ->
        Map("content" -> "[s7Post1] content", "id" -> 10, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7a"), "s5" ->
        Map("id" -> 5, "languages" -> Seq("language5"), "birthday" -> 5, "creationDate" -> 5, "lastName" -> "last5-ᚠさ丵פش", "browserUsed" -> "browser5", "email" -> Seq("s5@email.com"), "locationIP" -> "ip5", "gender" -> "gender5", "firstName" -> "s5"), "f3" ->
        Map("id" -> 3, "languages" -> Seq("language3a", "language3b"), "birthday" -> 3, "creationDate" -> 3, "lastName" -> "last3-ᚠさ丵פش", "browserUsed" -> "browser3", "email" -> Seq("f3a@email.com", "f3b@email.com"), "locationIP" -> "ip3", "gender" -> "gender3", "firstName" -> "f3"), "f2Post3" ->
        Map("content" -> "[f2Post3] content", "id" -> 7, "creationDate" -> 946940400000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "f2" ->
        Map("id" -> 2, "languages" -> Seq("language2"), "birthday" -> 2, "creationDate" -> 2, "lastName" -> "last2-ᚠさ丵פش", "browserUsed" -> "browser2", "email" -> Seq("f2@email.com"), "locationIP" -> "ip2", "gender" -> "gender2", "firstName" -> "f2"), "f2Post2" ->
        Map("content" -> "[f2Post2] content", "id" -> 6, "creationDate" -> 947458800000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "f2Post1" ->
        Map("content" -> "[f2Post1] content", "id" -> 5, "creationDate" -> 947458800000L, "browserUsed" -> "ip2", "locationIP" -> "ip2", "imageFile" -> "ip2", "language" -> "language2"), "s7Post2" ->
        Map("content" -> "[s7Post2] content", "id" -> 11, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7"), "f4" ->
        Map("id" -> 4, "languages" -> Seq("language4a", "language4b"), "birthday" -> 4, "creationDate" -> 4, "lastName" -> "last4-ᚠさ丵פش", "browserUsed" -> "browser4", "email" -> Seq("f4@email.com"), "locationIP" -> "ip4", "gender" -> "gender4", "firstName" -> "f4"), "country1" ->
        Map("id" -> 11, "name" -> "country1"), "person1" ->
        Map("id" -> 1, "languages" -> Seq("language1a", "language1b"), "birthday" -> 1, "creationDate" -> 1, "lastName" -> "last1-ᚠさ丵פش", "browserUsed" -> "browser1", "email" -> Seq("person1a@email.com", "person1b@email.com"), "locationIP" -> "ip1", "gender" -> "gender1", "firstName" -> "person1"), "country4" ->
        Map("id" -> 14, "name" -> "country4"), "country5" ->
        Map("id" -> 15, "name" -> "country5"), "country2" ->
        Map("id" -> 12, "name" -> "country2"), "country3" ->
        Map("id" -> 13, "name" -> "country3"), "ff6Post1" ->
        Map("content" -> "[ff6Post1] content", "id" -> 12, "creationDate" -> 946940400000L, "browserUsed" -> "browser6", "locationIP" -> "ip6", "imageFile" -> "image6", "language" -> "language6"), "f3Post1" ->
        Map("content" -> "[f3Post1] content", "id" -> 1, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post2" ->
        Map("content" -> "[f3Post2] content", "id" -> 2, "creationDate" -> 947458800000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post3" ->
        Map("content" -> "[f3Post3] content", "id" -> 3, "creationDate" -> 946940400000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f4Post1" ->
        Map("content" -> "[f4Post1] content", "id" -> 4, "creationDate" -> 946854000000L, "browserUsed" -> "browser4", "locationIP" -> "ip4", "imageFile" -> "image4", "language" -> "language4"))

    val query = """MATCH (countryX:Country {name:{2}}), (countryY:Country {name:{3}}), (person:Person {id:{1}})
                  |MATCH (person)-[:KNOWS*1..2]-(friend:Person)-[:PERSON_IS_LOCATED_IN]->()-[:IS_PART_OF]->(country:Country)
                  |WHERE not(person=friend) AND not(country=countryX) AND not(country=countryY)
                  |MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(messageX)-[:POST_IS_LOCATED_IN|COMMENT_IS_LOCATED_IN]->(countryX)
                  |WHERE messageX.creationDate>={4} AND messageX.creationDate<{5}
                  |WITH friend, countryY, count(DISTINCT messageX) AS xCount
                  |MATCH (friend)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(messageY)-[:POST_IS_LOCATED_IN|COMMENT_IS_LOCATED_IN]->(countryY)
                  |WHERE messageY.creationDate>={4} AND messageY.creationDate<{5}
                  |WITH friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName , xCount, count(DISTINCT messageY) AS yCount
                  |RETURN friendId, friendFirstName, friendLastName, xCount, yCount, xCount + yCount AS xyCount
                  |ORDER BY xyCount DESC, friendId ASC
                  |LIMIT {6}""".stripMargin

    def params = {
      val startTime = new DateTime(2000, 1, 3, 0, 0, 0, DateTimeZone.forID("Europe/Stockholm"))
      val endTime = startTime.plusDays(2)
      Map("1" -> 1, "2" -> "country1", "3" -> "country2", "4" -> startTime.getMillis, "5" -> endTime.getMillis, "6" -> 10)
    }

    def expectedResult = List(
      Map("friendLastName" -> "last2-ᚠさ丵פش", "friendId" -> 2, "friendFirstName" -> "f2", "yCount" -> 1, "xyCount" -> 2, "xCount" -> 1),
      Map("friendLastName" -> "last6-ᚠさ丵פش", "friendId" -> 6, "friendFirstName" -> "ff6", "yCount" -> 1, "xyCount" -> 2, "xCount" -> 1))
  }

  object Query4 extends LdbcQuery {

    val name = "LDBC Query 4"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (tag1:Tag {tag1}), (tag2:Tag {tag2}), (tag3:Tag {tag3}), (tag4:Tag {tag4}), (tag5:Tag {tag5}),
                        | (person1:Person {person1}), (f2:Person {f2}), (f3:Person {f3}), (f4:Person {f4}),
                        |(s5:Person {s5}), (ff6:Person {ff6}),(s7:Person {s7}),
                        | (f3Post1:Post {f3Post1}), (f3Post2:Post {f3Post2}), (f3Post3:Post {f3Post3}),
                        | (f4Post1:Post {f4Post1}), (f2Post1:Post {f2Post1}), (f2Post2:Post {f2Post2}), (f2Post3:Post {f2Post3}),
                        | (s5Post1:Post {s5Post1}), (s5Post2:Post {s5Post2}), (ff6Post1:Post {ff6Post1}),
                        | (s7Post1:Post {s7Post1}), (s7Post2:Post {s7Post2}),
                        | (f4Comment1:Comment {f4Comment1})
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:KNOWS]->(n) )
                        |FOREACH (n IN [ff6] | CREATE (f2)-[:KNOWS]->(n) )
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [f4Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(f4) )
                        |FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s7) )
                        |FOREACH (n IN [ff6Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(ff6) )
                        |FOREACH (n IN [f4Comment1] | CREATE (n)-[:COMMENT_HAS_CREATOR]->(f4) )
                        |FOREACH (n IN [f4Comment1] | CREATE (n)-[:REPLY_OF_POST]->(f2Post1) )
                        |FOREACH (n IN [f3Post1,f3Post2,f2Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag4) )
                        |FOREACH (n IN [f3Post3,ff6Post1,s7Post2] | CREATE (n)-[:POST_HAS_TAG]->(tag5) )
                        |FOREACH (n IN [f3Post3,f4Post1,f2Post2,s5Post2,ff6Post1,s7Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag3) )
                        |FOREACH (n IN [f3Post3,f4Post1,f2Post1,f2Post3,s5Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag2) )
                        |FOREACH (n IN [f3Post1,f2Post1,f2Post3,s5Post1,ff6Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag1) )
                        |FOREACH (n IN [f4Comment1] | CREATE (n)-[:COMMENT_HAS_TAG]->(tag1) )""".stripMargin

    def createParams =
      Map("person1" ->
        Map("id" -> 1, "languages" -> Seq("language1a", "language1b"), "birthday" -> 1, "creationDate" -> 1, "lastName" -> "last1", "browserUsed" -> "browser1", "email" -> Seq("person1b@email.com", "person1b@email.com"), "locationIP" -> "ip1", "gender" -> "gender1", "firstName" -> "person1"), "s5Post1" ->
        Map("content" -> "[s5Post1] content", "id" -> 8, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "f4Comment1" ->
        Map("content" -> "[f4Comment1] content", "id" -> 13, "creationDate" -> 946854000000L, "browserUsed" -> "browser4", "locationIP" -> "ip4"), "ff6Post1" ->
        Map("content" -> "[ff6Post1] content", "id" -> 12, "creationDate" -> 946854000000L, "browserUsed" -> "browser6", "locationIP" -> "ip6", "imageFile" -> "image6", "language" -> "language6"), "f3Post1" ->
        Map("content" -> "[f3Post1] content", "id" -> 1, "creationDate" -> 946767600000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post2" ->
        Map("content" -> "[f3Post2] content", "id" -> 2, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post3" ->
        Map("content" -> "[f3Post3] content", "id" -> 3, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "ff6" ->
        Map("id" -> 6, "languages" -> Seq("language6a", "language6b"), "birthday" -> 1, "creationDate" -> 1, "lastName" -> "last6", "browserUsed" -> "browser6", "email" -> Seq("friend6@email.com"), "locationIP" -> "ip6", "gender" -> "gender6", "firstName" -> "ff6"), "tag4" ->
        Map("name" -> "tag4-ᚠさ丵פش"), "tag5" ->
        Map("name" -> "tag5-ᚠさ丵פش"), "tag2" ->
        Map("name" -> "tag2-ᚠさ丵פش"), "tag3" ->
        Map("name" -> "tag3-ᚠさ丵פش"), "s7" ->
        Map("id" -> 7, "languages" -> Seq("language7a", "language7b"), "birthday" -> 7, "creationDate" -> 7, "lastName" -> "last7", "browserUsed" -> "browser7", "email" -> Seq("stranger7a@email.com", "stranger7b@email.com"), "locationIP" -> "ip7", "gender" -> "gender7", "firstName" -> "s7"), "s5Post2" ->
        Map("content" -> "[s5Post2] content", "id" -> 9, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "f4Post1" ->
        Map("content" -> "[f4Post1] content", "id" -> 4, "creationDate" -> 947026800000L, "browserUsed" -> "browser4", "locationIP" -> "ip4", "imageFile" -> "image4", "language" -> "language4"), "s7Post1" ->
        Map("content" -> "[s7Post1] content", "id" -> 10, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7a"), "f2Post3" ->
        Map("content" -> "[f2Post3] content", "id" -> 7, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "f3" ->
        Map("id" -> 3, "languages" -> Seq("language3a", "language3b"), "birthday" -> 3, "creationDate" -> 3, "lastName" -> "last3", "browserUsed" -> "browser3", "email" -> Seq("friend3a@email.com", "friend3b@email.com"), "locationIP" -> "ip3", "gender" -> "gender3", "firstName" -> "f3"), "s5" ->
        Map("id" -> 5, "languages" -> Seq("language5"), "birthday" -> 5, "creationDate" -> 5, "lastName" -> "last5", "browserUsed" -> "browser5", "email" -> Seq("stranger5@email.com"), "locationIP" -> "ip5", "gender" -> "gender5", "firstName" -> "s5"), "f2Post2" ->
        Map("content" -> "[f2Post2] content", "id" -> 6, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "tag1" ->
        Map("name" -> "tag1-ᚠさ丵פش"), "f2" ->
        Map("id" -> 2, "languages" -> Seq("language2"), "birthday" -> 2, "creationDate" -> 2, "lastName" -> "last2", "browserUsed" -> "browser2", "email" -> Seq("friend2@email.com"), "locationIP" -> "ip2", "gender" -> "gender2", "firstName" -> "f2"), "f2Post1" ->
        Map("content" -> "[f2Post1] content", "id" -> 5, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "s7Post2" ->
        Map("content" -> "[s7Post2] content", "id" -> 11, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7"), "f4" ->
        Map("id" -> 4, "languages" -> Seq("language4a", "language4b"), "birthday" -> 1, "creationDate" -> 1, "lastName" -> "last4", "browserUsed" -> "browser4", "email" -> Seq("friend4@email.com"), "locationIP" -> "ip4", "gender" -> "gender4", "firstName" -> "f4"))


    val query = """MATCH (person:Person {id:{1}})-[:KNOWS]-(:Person)<-[:POST_HAS_CREATOR]-(post:Post)-[:POST_HAS_TAG]->(tag:Tag)
                  |WHERE post.creationDate >= {2} AND post.creationDate < {3}
                  |OPTIONAL MATCH (tag)<-[:POST_HAS_TAG]-(oldPost:Post)
                  |WHERE oldPost.creationDate < {2}
                  |WITH tag, post, length(collect(oldPost)) AS oldPostCount
                  |WHERE oldPostCount=0
                  |RETURN tag.name AS tagName, length(collect(post)) AS postCount
                  |ORDER BY postCount DESC, tagName ASC
                  |LIMIT {4}
                  | """.stripMargin

    def params = {
      val startTime = new DateTime(2000, 1, 3, 0, 0, 0,  DateTimeZone.forID("Europe/Stockholm"))
      val endTime = startTime.plusDays(2)
      Map("1" -> 1, "2" -> startTime.getMillis, "3" -> endTime.getMillis, "4" -> 10)
    }

    def expectedResult = List(
      Map("tagName" -> "tag2-ᚠさ丵פش", "postCount" -> 3),
      Map("tagName" -> "tag3-ᚠさ丵פش", "postCount" -> 2),
      Map("tagName" -> "tag5-ᚠさ丵פش", "postCount" -> 1))
  }

  object Query5 extends LdbcQuery {

    val name = "LDBC Query 5"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (forum1:Forum {forum1}), (forum2:Forum {forum2}),
                        | (forum3:Forum {forum3}), (forum4:Forum {forum4}),
                        | (person1:Person {person1}), (f2:Person {f2}), (f3:Person {f3}), (f4:Person {f4}),
                        | (s5:Person {s5}), (ff6:Person {ff6}),(s7:Person {s7}),
                        | (country0:Country {country0}),
                        | (country1:Country {country1}),
                        | (f3Post1:Post {f3Post1}), (f3Post2:Post {f3Post2}), (f3Post3:Post {f3Post3}),
                        | (f2Post1:Post {f2Post1}), (f2Post2:Post {f2Post2}), (f2Post3:Post {f2Post3}),
                        | (s5Post1:Post {s5Post1}), (s5Post2:Post {s5Post2}), (ff6Post1:Post {ff6Post1}),
                        | (s7Post1:Post {s7Post1}), (s7Post2:Post {s7Post2}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        | (forum1)-[:HAS_MEMBER {forum1HasMemberPerson1}]->(person1), (forum1)-[:HAS_MEMBER {forum1HasMemberF2}]->(f2),
                        | (forum1)-[:HAS_MEMBER {forum1HasMemberS5}]->(s5), (forum1)-[:HAS_MEMBER {forum1HasMemberF3}]->(f3),
                        | (forum1)-[:HAS_MEMBER {forum1HasMemberFF6}]->(ff6),
                        | (forum2)-[:HAS_MEMBER {forum2HasMemberF3}]->(f3), (forum3)-[:HAS_MEMBER {forum3HasMemberPerson1}]->(person1),
                        | (forum3)-[:HAS_MEMBER {forum3HasMemberF3}]->(f3), (forum3)-[:HAS_MEMBER {forum3HasMemberF4}]->(f4),
                        | (forum4)-[:HAS_MEMBER {forum4HasMemberF2}]->(f2),
                        | (forum4)-[:HAS_MEMBER {forum4HasMemberPerson1}]->(person1),
                        | (person1)-[:KNOWS]->(f2), (f2)-[:KNOWS]->(ff6),
                        | (person1)-[:KNOWS]->(f3),
                        | (person1)-[:KNOWS]->(f4)-[:KNOWS]->(f2),
                        | (f4)-[:KNOWS]->(ff6)
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [] | CREATE (n)-[:POST_HAS_CREATOR]->(f4) )
                        |FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s7) )
                        |FOREACH (n IN [ff6Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(ff6) )
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3, s5Post1, s5Post2, ff6Post1] | CREATE (n)-[:POST_IS_LOCATED_IN]->(country0) )
                        |FOREACH (n IN [f2Post1, f2Post2, f2Post3, s7Post1, s7Post2] | CREATE (n)-[:POST_IS_LOCATED_IN]->(country1) )
                        |FOREACH (n IN [f3Post1, f3Post2, f2Post1, f2Post2, s5Post1, s5Post2, ff6Post1, s7Post1, s7Post2]| CREATE (forum1)-[:CONTAINER_OF]->(n) )
                        |FOREACH (n IN [f3Post3] | CREATE (forum3)-[:CONTAINER_OF]->(n) )
                        |FOREACH (n IN [f2Post3] | CREATE (forum4)-[:CONTAINER_OF]->(n) )""".stripMargin

    def createParams = Map("forum4HasMemberPerson1" ->
      Map("joinDate" -> 946854000000L), "s5Post1" ->
      Map("content" -> "[s5Post1] content", "id" -> 8L, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "forum2HasMemberF3" ->
      Map("joinDate" -> 946854000000L), "forum3HasMemberF3" ->
      Map("joinDate" -> 946854000000L), "forum3HasMemberPerson1" ->
      Map("joinDate" -> 946854000000L), "ff6" ->
      Map("id" -> 6L, "languages" -> Seq("language6a", "language6b"), "birthday" -> 6L, "creationDate" -> 6L, "lastName" -> "last6", "browserUsed" -> "browser6", "email" -> Seq("ff6@email.com"), "locationIP" -> "ip6", "gender" -> "gender6", "firstName" -> "ff6"), "forum3HasMemberF4" ->
      Map("joinDate" -> 946854000000L), "forum4HasMemberF2" ->
      Map("joinDate" -> 946681200000L), "s7" ->
      Map("id" -> 7L, "languages" -> Seq("language7a", "language7b"), "birthday" -> 7L, "creationDate" -> 7L, "lastName" -> "last7", "browserUsed" -> "browser7", "email" -> Seq("s7@email.com"), "locationIP" -> "ip7", "gender" -> "gender7", "firstName" -> "s7"), "forum1HasMemberFF6" ->
      Map("joinDate" -> 946854000000L), "s5Post2" ->
      Map("content" -> "[s5Post2] content", "id" -> 9L, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "forum1HasMemberS5" ->
      Map("joinDate" -> 946854000000L), "s5" ->
      Map("id" -> 5L, "languages" -> Seq("language5"), "birthday" -> 5L, "creationDate" -> 5L, "lastName" -> "last5", "browserUsed" -> "browser5", "email" -> Seq("s5@email.com"), "locationIP" -> "ip5", "gender" -> "gender5", "firstName" -> "s5"), "f3" ->
      Map("id" -> 3L, "languages" -> Seq("language3a", "language3b"), "birthday" -> 3L, "creationDate" -> 3L, "lastName" -> "last3", "browserUsed" -> "browser3", "email" -> Seq("f3a@email.com", "f3b@email.com"), "locationIP" -> "ip3", "gender" -> "gender3", "firstName" -> "f3"), "f2Post3" ->
      Map("content" -> "[f2Post3] content", "id" -> 7L, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "s7Post1" ->
      Map("content" -> "[s7Post1] content", "id" -> 10L, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7a"), "f2" ->
      Map("id" -> 2L, "languages" -> Seq("language2"), "birthday" -> 2L, "creationDate" -> 2L, "lastName" -> "last2", "browserUsed" -> "browser2", "email" -> Seq("f2@email.com"), "locationIP" -> "ip2", "gender" -> "gender2", "firstName" -> "f2"), "f2Post2" ->
      Map("content" -> "[f2Post2] content", "id" -> 6L, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "f2Post1" ->
      Map("content" -> "[f2Post1] content", "id" -> 5L, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "f4" ->
      Map("id" -> 4L, "languages" -> Seq("language4a", "language4b"), "birthday" -> 4L, "creationDate" -> 4L, "lastName" -> "last4", "browserUsed" -> "browser4", "email" -> Seq("f4@email.com"), "locationIP" -> "ip4", "gender" -> "gender4", "firstName" -> "f4"), "s7Post2" ->
      Map("content" -> "[s7Post2] content", "id" -> 11L, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7"), "country0" ->
      Map("id" -> 10L, "name" -> "country0"), "country1" ->
      Map("id" -> 11L, "name" -> "country1"), "forum1HasMemberPerson1" ->
      Map("joinDate" -> 946854000000L), "person1" ->
      Map("id" -> 1L, "languages" -> Seq("language1a", "language1b"), "birthday" -> 1L, "creationDate" -> 1L, "lastName" -> "last1", "browserUsed" -> "browser1", "email" -> Seq("person1a@email.com", "person1b@email.com"), "locationIP" -> "ip1", "gender" -> "gender1", "firstName" -> "person1"), "forum1" ->
      Map("id" -> 1L, "title" -> "forum1-ᚠさ丵פش"), "forum2" ->
      Map("id" -> 2L, "title" -> "forum1-ᚠさ丵פش"), "forum3" ->
      Map("id" -> 3L, "title" -> "forum3-ᚠさ丵פش"), "forum4" ->
      Map("id" -> 4L, "title" -> "forum4-ᚠさ丵פش"), "ff6Post1" ->
      Map("content" -> "[ff6Post1] content", "id" -> 12L, "creationDate" -> 946854000000L, "browserUsed" -> "browser6", "locationIP" -> "ip6", "imageFile" -> "image6", "language" -> "language6"), "f3Post1" ->
      Map("content" -> "[f3Post1] content", "id" -> 1L, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post2" ->
      Map("content" -> "[f3Post2] content", "id" -> 2L, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post3" ->
      Map("content" -> "[f3Post3] content", "id" -> 3L, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "forum1HasMemberF2" ->
      Map("joinDate" -> 946681200000L), "forum1HasMemberF3" ->
      Map("joinDate" -> 946681200000L))

    val query = """MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[membership:HAS_MEMBER]-(forum:Forum)
                  |WHERE membership.joinDate>{2} AND not(person=friend)
                  |WITH DISTINCT friend, forum
                  |OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)<-[:CONTAINER_OF]-(forum)
                  |WITH forum, count(post) AS postCount
                  |RETURN forum.title AS forumName, postCount
                  |ORDER BY postCount DESC, forum.id ASC
                  |LIMIT {3}""".stripMargin

    def params = {
      val startTime = new DateTime(2000, 1, 2, 0, 0, 0)
      Map("1" -> 1, "2" -> startTime.getMillis, "3" -> 4)
    }

    def expectedResult = List(Map("forumName" -> "forum1-ᚠさ丵פش", "postCount" -> 1), Map("forumName" -> "forum3-ᚠさ丵פش", "postCount" -> 1), Map("forumName" -> "forum1-ᚠさ丵פش", "postCount" -> 0))

  }

  object Query6 extends LdbcQuery {

    val name = "LDBC Query 6"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (tag1:Tag {tag1}), (tag2:Tag {tag2}), (tag3:Tag {tag3}), (tag4:Tag {tag4}), (tag5:Tag {tag5}),
                        | (person1:Person {person1}), (f2:Person {f2}), (f3:Person {f3}), (f4:Person {f4}),
                        | (s5:Person {s5}), (ff6:Person {ff6}),(s7:Person {s7}),
                        | (f3Post1:Post {f3Post1}), (f3Post2:Post {f3Post2}), (f3Post3:Post {f3Post3}),
                        | (f4Post1:Post {f4Post1}), (f2Post1:Post {f2Post1}), (f2Post2:Post {f2Post2}), (f2Post3:Post {f2Post3}),
                        | (s5Post1:Post {s5Post1}), (s5Post2:Post {s5Post2}), (ff6Post1:Post {ff6Post1}),
                        | (s7Post1:Post {s7Post1}), (s7Post2:Post {s7Post2})
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |FOREACH (n IN [f3, f2, f4] | CREATE (person1)-[:KNOWS]->(n) )
                        |FOREACH (n IN [ff6] | CREATE (f2)-[:KNOWS]->(n) )
                        |FOREACH (n IN [f3Post1, f3Post2, f3Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f3) )
                        |FOREACH (n IN [f2Post1, f2Post2, f2Post3] | CREATE (n)-[:POST_HAS_CREATOR]->(f2) )
                        |FOREACH (n IN [f4Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(f4) )
                        |FOREACH (n IN [s5Post1, s5Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s5) )
                        |FOREACH (n IN [s7Post1, s7Post2] | CREATE (n)-[:POST_HAS_CREATOR]->(s7) )
                        |FOREACH (n IN [ff6Post1] | CREATE (n)-[:POST_HAS_CREATOR]->(ff6) )
                        |FOREACH (n IN [f3Post1,f3Post2,f2Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag4) )
                        |FOREACH (n IN [f3Post3,ff6Post1,s7Post2] | CREATE (n)-[:POST_HAS_TAG]->(tag5) )
                        |FOREACH (n IN [f3Post3,f4Post1,f2Post2,s5Post2,ff6Post1,s7Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag3) )
                        |FOREACH (n IN [f3Post3,f4Post1,f2Post1,f2Post3,s5Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag2) )
                        |FOREACH (n IN [f3Post1,f2Post1,f2Post3,s5Post1,ff6Post1] | CREATE (n)-[:POST_HAS_TAG]->(tag1) )""".stripMargin

    def createParams = Map("person1" ->
      Map("id" -> 1L, "languages" -> Seq("language1"), "birthday" -> 1L, "creationDate" -> 1L, "lastName" -> "last1", "browserUsed" -> "browser1", "email" -> Seq("person1@email.com"), "locationIP" -> "ip1", "gender" -> "gender1", "firstName" -> "person1"), "s5Post1" ->
      Map("content" -> "[s5Post1] content", "id" -> 8L, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "ff6Post1" ->
      Map("content" -> "[ff6Post1] content", "id" -> 12L, "creationDate" -> 946854000000L, "browserUsed" -> "browser6", "locationIP" -> "ip6", "imageFile" -> "image6", "language" -> "language6"), "f3Post1" ->
      Map("content" -> "[f3Post1] content", "id" -> 1L, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post2" ->
      Map("content" -> "[f3Post2] content", "id" -> 2L, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "f3Post3" ->
      Map("content" -> "[f3Post3] content", "id" -> 3L, "creationDate" -> 946854000000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "image3", "language" -> "language3"), "ff6" ->
      Map("id" -> 6L, "languages" -> Seq("language6"), "birthday" -> 6L, "creationDate" -> 6L, "lastName" -> "last6", "browserUsed" -> "browser6", "email" -> Seq("ff6@email.com"), "locationIP" -> "ip6", "gender" -> "gender6", "firstName" -> "ff6"), "tag4" ->
      Map("name" -> "tag4-ᚠさ丵פش"), "tag5" ->
      Map("name" -> "tag5-ᚠさ丵פش"), "tag2" ->
      Map("name" -> "tag2-ᚠさ丵פش"), "tag3" ->
      Map("name" -> "tag3-ᚠさ丵פش"), "s7" ->
      Map("id" -> 7L, "languages" -> Seq("language7"), "birthday" -> 7L, "creationDate" -> 7L, "lastName" -> "last7", "browserUsed" -> "browser7", "email" -> Seq("s7@email.com"), "locationIP" -> "ip7", "gender" -> "gender7", "firstName" -> "s7"), "s5Post2" ->
      Map("content" -> "[s5Post2] content", "id" -> 9L, "creationDate" -> 946854000000L, "browserUsed" -> "browser5", "locationIP" -> "ip5", "imageFile" -> "image5", "language" -> "language5"), "f4Post1" ->
      Map("content" -> "[f4Post1] content", "id" -> 4L, "creationDate" -> 946854000000L, "browserUsed" -> "browser4", "locationIP" -> "ip4", "imageFile" -> "image4", "language" -> "language4"), "s7Post1" ->
      Map("content" -> "[s7Post1] content", "id" -> 10L, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7"), "f2Post3" ->
      Map("content" -> "[f2Post3] content", "id" -> 7L, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "f3" ->
      Map("id" -> 3L, "languages" -> Seq("language3"), "birthday" -> 3L, "creationDate" -> 3L, "lastName" -> "last3", "browserUsed" -> "browser3", "email" -> Seq("f3@email.com"), "locationIP" -> "ip3", "gender" -> "gender3", "firstName" -> "f3"), "s5" ->
      Map("id" -> 5L, "languages" -> Seq("language5"), "birthday" -> 5L, "creationDate" -> 5L, "lastName" -> "last5", "browserUsed" -> "browser5", "email" -> Seq("f5@email.com"), "locationIP" -> "ip5", "gender" -> "gender5", "firstName" -> "s5"), "f2Post2" ->
      Map("content" -> "[f2Post2] content", "id" -> 6L, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "tag1" ->
      Map("name" -> "tag1-ᚠさ丵פش"), "f2" ->
      Map("id" -> 2L, "languages" -> Seq("language2"), "birthday" -> 2L, "creationDate" -> 2L, "lastName" -> "last2", "browserUsed" -> "browser2", "email" -> Seq("f2@email.com"), "locationIP" -> "ip2", "gender" -> "gender2", "firstName" -> "f2"), "f2Post1" ->
      Map("content" -> "[f2Post1] content", "id" -> 5L, "creationDate" -> 946854000000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "image2", "language" -> "language2"), "s7Post2" ->
      Map("content" -> "[s7Post2] content", "id" -> 11L, "creationDate" -> 946854000000L, "browserUsed" -> "browser7", "locationIP" -> "ip7", "imageFile" -> "image7", "language" -> "language7"), "f4" ->
      Map("id" -> 4L, "languages" -> Seq("language4"), "birthday" -> 4L, "creationDate" -> 4L, "lastName" -> "last4", "browserUsed" -> "browser4", "email" -> Seq("f4@email.com"), "locationIP" -> "ip4", "gender" -> "gender4", "firstName" -> "f4"))

    val query = """MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[:POST_HAS_CREATOR]-(friendPost:Post)-[:POST_HAS_TAG]->(knownTag:Tag {name:{2}})
                  |WHERE not(person=friend)
                  |MATCH (friendPost)-[:POST_HAS_TAG]->(commonTag:Tag)
                  |WHERE not(commonTag=knownTag)
                  |WITH DISTINCT commonTag, knownTag, friend
                  |MATCH (commonTag)<-[:POST_HAS_TAG]-(commonPost:Post)-[:POST_HAS_TAG]->(knownTag)
                  |WHERE (commonPost)-[:POST_HAS_CREATOR]->(friend)
                  |RETURN commonTag.name AS tagName, count(commonPost) AS postCount
                  |ORDER BY postCount DESC, tagName ASC
                  |LIMIT {3}""".stripMargin

    def params = Map("1" -> 1, "2" -> "tag3-ᚠさ丵פش", "3" -> 4)


    def expectedResult = List(Map("tagName" -> "tag2-ᚠさ丵פش", "postCount" -> 2), Map("tagName" -> "tag5-ᚠさ丵פش", "postCount" -> 2), Map("tagName" -> "tag1-ᚠさ丵פش", "postCount" -> 1))


  }

  object Query7 extends LdbcQuery {

    val name = "LDBC Query 7"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        |(person1:Person {person1}),
                        |(f2:Person {f2}),
                        |(f3:Person {f3}),
                        |(f4:Person {f4}),
                        |(ff5:Person {ff5}),
                        |(ff6:Person {ff6}),
                        |(s7:Person {s7}),
                        |(s8:Person {s8}),
                        |(person1Post1:Post {person1Post1}),
                        |(person1Post2:Post {person1Post2}),
                        |(person1Post3:Post {person1Post3}),
                        |(s7Post1:Post {s7Post1}),
                        | (city0:City {city0}),
                        |(person1Comment1:Comment {person1Comment1}),
                        |(f4Comment1:Comment {f4Comment1}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(person1)-[:KNOWS]->(f2),
                        |(person1)-[:KNOWS]->(f3),
                        |(person1)-[:KNOWS]->(f4),
                        |(f2)-[:KNOWS]->(ff5),
                        |(f3)-[:KNOWS]->(ff6),
                        | (person1)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f2)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f3)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f4)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff5)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff6)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (s7)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (s8)-[:PERSON_IS_LOCATED_IN]->(city0),
                        |(person1Comment1)-[:REPLY_OF_POST]->(s7Post1),
                        |(f4Comment1)-[:REPLY_OF_POST]->(person1Post3),
                        |(person1)-[:LIKES_POST {person1LikesPerson1Post1}]->(person1Post1),
                        |(person1)-[:LIKES_POST {person1LikesS7Post1}]->(s7Post1),
                        |(f2)-[:LIKES_POST {f2LikesPerson1Post1}]->(person1Post1),
                        |(f4)-[:LIKES_POST {f4LikesPerson1Post1}]->(person1Post1),
                        |(f4)-[:LIKES_POST {f4LikesPerson1Post2}]->(person1Post2),
                        |(f4)-[:LIKES_POST {f4LikesPerson1Post3}]->(person1Post3),
                        |(ff6)-[:LIKES_POST {ff6OldLikesPerson1Post1}]->(person1Post1),
                        |(ff6)-[:LIKES_POST {ff6NewLikesPerson1Post1}]->(person1Post1),
                        |(ff6)-[:LIKES_POST {ff6LikesPerson1Post2}]->(person1Post2),
                        |(s7)-[:LIKES_POST {s7LikesPerson1Post1}]->(person1Post1),
                        |(s8)-[:LIKES_POST {s8LikesPerson1Post2}]->(person1Post2),
                        |(person1)-[:LIKES_COMMENT {person1LikesF4Comment1}]->(f4Comment1),
                        |(s7)-[:LIKES_COMMENT {s7LikesPerson1Comment1}]->(person1Comment1),
                        |(s8)-[:LIKES_COMMENT {s8LikesF4Comment1}]->(f4Comment1),
                        |(person1)<-[:POST_HAS_CREATOR]-(person1Post1),
                        |(person1)<-[:POST_HAS_CREATOR]-(person1Post2),
                        |(person1)<-[:POST_HAS_CREATOR]-(person1Post3),
                        |(s7)<-[:POST_HAS_CREATOR]-(s7Post1),
                        |(person1)<-[:COMMENT_HAS_CREATOR]-(person1Comment1),
                        |(f4)<-[:COMMENT_HAS_CREATOR]-(f4Comment1)""".stripMargin


    val query = """MATCH (person:Person {id:{1}})<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)<-[like:LIKES_POST|LIKES_COMMENT]-(liker:Person)
                  |    WITH liker, message, like.creationDate AS likeCreationDate, person
                  |    ORDER BY likeCreationDate DESC, message.id ASC
                  |      WITH liker, head(collect({msg: message, likeTime: likeCreationDate})) AS latestLike, person
                  |    RETURN liker.id AS personId, liker.firstName AS personFirstName, liker.lastName AS personLastName,
                  |    latestLike.likeTime AS likeTime, not((liker)-[:KNOWS]-(person)) AS isNew, latestLike.msg.id AS messageId,
                  |    CASE has(latestLike.msg.content) WHEN true THEN latestLike.msg.content
                  |    ELSE latestLike.msg.imageFile END AS messageContent, latestLike.likeTime - latestLike.msg.creationDate AS latencyAsMilli
                  |    ORDER BY likeTime DESC, personId ASC
                  |      LIMIT {2}""".stripMargin

    def createParams = Map("person1Post2" ->
      Map("content" -> "person1post2", "id" -> 2L, "creationDate" -> 946681320000L, "browserUsed" -> "browser2", "locationIP" -> "ip2", "imageFile" -> "imageFile2", "language" -> "language2"), "person1Post3" ->
      Map("content" -> "person1post3", "id" -> 3L, "creationDate" -> 946681380000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "imageFile3", "language" -> "language3"), "person1Post1" ->
      Map("content" -> "person1post1", "id" -> 1L, "creationDate" -> 946681260000L, "browserUsed" -> "browser1", "locationIP" -> "ip1", "imageFile" -> "imageFile1", "language" -> "language1"), "f4Comment1" ->
      Map("content" -> "f4comment1", "id" -> 6L, "creationDate" -> 946681560000L, "browserUsed" -> "browser6", "locationIP" -> "ip6"), "s7LikesPerson1Comment1" ->
      Map("creationDate" -> 946681560000L), "city0" ->
      Map("id" -> 0L, "name" -> "city0"), "person1Comment1" ->
      Map("content" -> "person1comment1", "id" -> 5L, "creationDate" -> 946681500000L, "browserUsed" -> "browser5", "locationIP" -> "ip5"), "ff5" ->
      Map("id" -> 5L, "languages" -> Seq("language5"), "birthday" -> 5L, "creationDate" -> 5L, "lastName" -> "last5-ᚠさ丵פش", "browserUsed" -> "browser5", "email" -> Seq("ff5@email.com"), "locationIP" -> "ip5", "gender" -> "gender5", "firstName" -> "ff5"), "ff6" ->
      Map("id" -> 6L, "languages" -> Seq("language6"), "birthday" -> 6L, "creationDate" -> 6L, "lastName" -> "last6-ᚠさ丵פش", "browserUsed" -> "browser6", "email" -> Seq("ff6@email.com"), "locationIP" -> "ip6", "gender" -> "gender6", "firstName" -> "ff6"), "person1LikesF4Comment1" ->
      Map("creationDate" -> 946682400000L), "ff6LikesPerson1Post2" ->
      Map("creationDate" -> 946681380000L), "s7" ->
      Map("id" -> 7L, "languages" -> Seq("language7"), "birthday" -> 7L, "creationDate" -> 7L, "lastName" -> "last7-ᚠさ丵פش", "browserUsed" -> "browser7", "email" -> Seq("s7@email.com"), "locationIP" -> "ip7", "gender" -> "gender7", "firstName" -> "s7"), "s8" ->
      Map("id" -> 8L, "languages" -> Seq("language8"), "birthday" -> 8L, "creationDate" -> 8L, "lastName" -> "last8-ᚠさ丵פش", "browserUsed" -> "browser8", "email" -> Seq("s8@email.com"), "locationIP" -> "ip8", "gender" -> "gender8", "firstName" -> "s8"), "s7Post1" ->
      Map("content" -> "s7post1", "id" -> 4L, "creationDate" -> 946681440000L, "browserUsed" -> "browser3", "locationIP" -> "ip3", "imageFile" -> "imageFile4", "language" -> "language4"), "f3" ->
      Map("id" -> 3L, "languages" -> Seq("language3"), "birthday" -> 3L, "creationDate" -> 3L, "lastName" -> "last3-ᚠさ丵פش", "browserUsed" -> "browser3", "email" -> Seq("f3@email.com"), "locationIP" -> "ip3", "gender" -> "gender3", "firstName" -> "f3"), "f2" ->
      Map("id" -> 2L, "languages" -> Seq("language2"), "birthday" -> 2L, "creationDate" -> 2L, "lastName" -> "last2-ᚠさ丵פش", "browserUsed" -> "browser2", "email" -> Seq("f2@email.com"), "locationIP" -> "ip2", "gender" -> "gender2", "firstName" -> "f2"), "f4" ->
      Map("id" -> 4L, "languages" -> Seq("language4"), "birthday" -> 4L, "creationDate" -> 4L, "lastName" -> "last4-ᚠさ丵פش", "browserUsed" -> "browser4", "email" -> Seq("f4@email.com"), "locationIP" -> "ip4", "gender" -> "gender4", "firstName" -> "f4"), "person1LikesPerson1Post1" ->
      Map("creationDate" -> 946681260000L), "person1" ->
      Map("id" -> 1L, "languages" -> Seq("language1"), "birthday" -> 2L, "creationDate" -> 1L, "lastName" -> "last1-ᚠさ丵פش", "browserUsed" -> "browser1", "email" -> Seq("person1@email.com"), "locationIP" -> "ip1", "gender" -> "gender1", "firstName" -> "person1"), "s8LikesF4Comment1" ->
      Map("creationDate" -> 946682400000L), "s8LikesPerson1Post2" ->
      Map("creationDate" -> 946681800000L), "s7LikesPerson1Post1" ->
      Map("creationDate" -> 946681320000L), "f4LikesPerson1Post3" ->
      Map("creationDate" -> 946681500000L), "f4LikesPerson1Post2" ->
      Map("creationDate" -> 946681440000L), "f4LikesPerson1Post1" ->
      Map("creationDate" -> 946681380000L), "person1LikesS7Post1" ->
      Map("creationDate" -> 946682400000L), "f2LikesPerson1Post1" ->
      Map("creationDate" -> 946681500000L), "ff6NewLikesPerson1Post1" ->
      Map("creationDate" -> 946681440000L), "ff6OldLikesPerson1Post1" ->
      Map("creationDate" -> 946681320000L))


    def params = Map("1" -> 1, "2" -> 1)

    def expectedResult = List(
      Map("isNew" -> true, "likeTime" -> 946681800000L, "personId" -> 8, "latencyAsMilli" -> 480000, "messageId" -> 2,
        "personLastName" -> "last8-ᚠさ丵פش", "messageContent" -> "person1post2", "personFirstName" -> "s8"))
  }

  object Query8 extends LdbcQuery {

    val name = "LDBC Query 8"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person:Person {person}),
                        | (friend1:Person {friend1}),
                        | (friend2:Person {friend2}),
                        | (friend3:Person {friend3}),
                        | (post0:Post {post0}),
                        | (post1:Post {post1}),
                        | (post2:Post {post2}),
                        | (post3:Post {post3}),
                        | (comment01:Comment {comment01}),
                        | (comment11:Comment {comment11}),
                        | (comment12:Comment {comment12}),
                        | (comment13:Comment {comment13}),
                        | (comment131:Comment {comment131}),
                        | (comment111:Comment {comment111}),
                        | (comment112:Comment {comment112}),
                        | (comment21:Comment {comment21}),
                        | (comment211:Comment {comment211}),
                        | (comment2111:Comment {comment2111}),
                        | (comment31:Comment {comment31}),
                        | (comment32:Comment {comment32}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(person)<-[:POST_HAS_CREATOR]-(post0),
                        |(person)<-[:POST_HAS_CREATOR]-(post1),
                        |(person)<-[:POST_HAS_CREATOR]-(post2),
                        |(friend3)<-[:POST_HAS_CREATOR]-(post3),
                        |(person)<-[:COMMENT_HAS_CREATOR]-(comment13),
                        |(person)<-[:COMMENT_HAS_CREATOR]-(comment31),
                        |(friend1)<-[:COMMENT_HAS_CREATOR]-(comment111),
                        |(friend1)<-[:COMMENT_HAS_CREATOR]-(comment21),
                        |(friend1)<-[:COMMENT_HAS_CREATOR]-(comment2111),
                        |(friend2)<-[:COMMENT_HAS_CREATOR]-(comment211),
                        |(friend2)<-[:COMMENT_HAS_CREATOR]-(comment131),
                        |(friend2)<-[:COMMENT_HAS_CREATOR]-(comment112),
                        |(friend2)<-[:COMMENT_HAS_CREATOR]-(comment32),
                        |(friend3)<-[:COMMENT_HAS_CREATOR]-(comment11),
                        |(friend3)<-[:COMMENT_HAS_CREATOR]-(comment12),
                        |(friend3)<-[:COMMENT_HAS_CREATOR]-(comment01),
                        |(post0)<-[:REPLY_OF_POST]-(comment01),
                        |(post1)<-[:REPLY_OF_POST]-(comment11)<-[:REPLY_OF_COMMENT]-(comment111), (comment11)<-[:REPLY_OF_COMMENT]-(comment112),
                        |(post1)<-[:REPLY_OF_POST]-(comment12),
                        |(post1)<-[:REPLY_OF_POST]-(comment13)<-[:REPLY_OF_COMMENT]-(comment131),
                        |(post2)<-[:REPLY_OF_POST]-(comment21)<-[:REPLY_OF_COMMENT]-(comment211)<-[:REPLY_OF_COMMENT]-(comment2111),
                        |(post3)<-[:REPLY_OF_POST]-(comment31), (post3)<-[:REPLY_OF_COMMENT]-(comment32)
                        | """.stripMargin


    def createParams = Map("person" ->
      Map("id" -> 0L, "lastName" -> "zero-ᚠさ丵פش", "firstName" -> "person"), "comment13" ->
      Map("content" -> "C13", "id" -> 13L, "creationDate" -> 3L), "comment12" ->
      Map("content" -> "C12", "id" -> 12L, "creationDate" -> 2L), "comment112" ->
      Map("content" -> "C112", "id" -> 16L, "creationDate" -> 6L), "comment111" ->
      Map("content" -> "C111", "id" -> 15L, "creationDate" -> 5L), "comment01" ->
      Map("content" -> "C01", "id" -> 10L, "creationDate" -> 1L), "comment131" ->
      Map("content" -> "C131", "id" -> 14L, "creationDate" -> 4L), "post3" ->
      Map("content" -> "P3", "id" -> 3L), "post2" ->
      Map("content" -> "P2", "id" -> 2L), "post1" ->
      Map("content" -> "P1", "id" -> 1L), "post0" ->
      Map("content" -> "P0", "id" -> 0L), "friend1" ->
      Map("id" -> 1L, "lastName" -> "one-ᚠさ丵פش", "firstName" -> "friend"), "comment2111" ->
      Map("content" -> "C2111", "id" -> 19L, "creationDate" -> 9L), "comment11" ->
      Map("content" -> "C11", "id" -> 11L, "creationDate" -> 1L), "comment21" ->
      Map("content" -> "C21", "id" -> 17L, "creationDate" -> 7L), "comment31" ->
      Map("content" -> "C31", "id" -> 20L, "creationDate" -> 10L), "comment32" ->
      Map("content" -> "C32", "id" -> 21L, "creationDate" -> 2L), "friend3" ->
      Map("id" -> 3L, "lastName" -> "three-ᚠさ丵פش", "firstName" -> "friend"), "comment211" ->
      Map("content" -> "C211", "id" -> 18L, "creationDate" -> 8L), "friend2" ->
      Map("id" -> 2L, "lastName" -> "two-ᚠさ丵פش", "firstName" -> "friend"))

    val query = """MATCH (start:Person {id:{1}})<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-()<-[:REPLY_OF_POST|REPLY_OF_COMMENT]-(comment:Comment)-[:COMMENT_HAS_CREATOR]->(person:Person)
                  |RETURN person.id AS personId, person.firstName AS personFirstName, person.lastName AS personLastName, comment.id AS commentId, comment.creationDate AS commentCreationDate, comment.content AS commentContent
                  |ORDER BY commentCreationDate DESC, commentId ASC
                  |LIMIT {2}""".stripMargin

    def params = Map("1" -> 0, "2" -> 10)

    def expectedResult = List(
      Map("personId" -> 1, "commentContent" -> "C21", "commentId" -> 17, "personLastName" -> "one-ᚠさ丵פش", "commentCreationDate" -> 7, "personFirstName" -> "friend"),
      Map("personId" -> 2, "commentContent" -> "C131", "commentId" -> 14, "personLastName" -> "two-ᚠさ丵פش", "commentCreationDate" -> 4, "personFirstName" -> "friend"),
      Map("personId" -> 0, "commentContent" -> "C13", "commentId" -> 13, "personLastName" -> "zero-ᚠさ丵פش", "commentCreationDate" -> 3, "personFirstName" -> "person"),
      Map("personId" -> 3, "commentContent" -> "C12", "commentId" -> 12, "personLastName" -> "three-ᚠさ丵פش", "commentCreationDate" -> 2, "personFirstName" -> "friend"),
      Map("personId" -> 3, "commentContent" -> "C01", "commentId" -> 10, "personLastName" -> "three-ᚠさ丵פش", "commentCreationDate" -> 1, "personFirstName" -> "friend"),
      Map("personId" -> 3, "commentContent" -> "C11", "commentId" -> 11, "personLastName" -> "three-ᚠさ丵פش", "commentCreationDate" -> 1, "personFirstName" -> "friend"))
  }

  object Query9 extends LdbcQuery {

    val name = "LDBC Query 9"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person0:Person {person0}),
                        | (friend1:Person {friend1}),
                        | (friend2:Person {friend2}),
                        | (stranger3:Person {stranger3}),
                        | (friendfriend4:Person {friendfriend4}),
                        | (post01:Post {post01}),
                        | (post11:Post {post11}),
                        | (post12:Post {post12}),
                        | (post21:Post {post21}),
                        | (post31:Post {post31}),
                        | (comment111:Comment {comment111}),
                        | (comment121:Comment {comment121}),
                        | (comment1211:Comment {comment1211}),
                        | (comment211:Comment {comment211}),
                        | (comment2111:Comment {comment2111}),
                        | (comment21111:Comment {comment21111}),
                        | (comment311:Comment {comment311}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(person0)-[:KNOWS]->(friend1)-[:KNOWS]->(friendfriend4)<-[:KNOWS]-(friend2),
                        |(person0)-[:KNOWS]->(friend2)<-[:KNOWS]-(friend1),
                        |(person0)<-[:POST_HAS_CREATOR]-(post01),
                        |(friend1)<-[:POST_HAS_CREATOR]-(post11),
                        |(friend1)<-[:POST_HAS_CREATOR]-(post12),
                        |(friend2)<-[:POST_HAS_CREATOR]-(post21),
                        |(stranger3)<-[:POST_HAS_CREATOR]-(post31),
                        |(person0)<-[:COMMENT_HAS_CREATOR]-(comment111),
                        |(person0)<-[:COMMENT_HAS_CREATOR]-(comment121),
                        |(friend1)<-[:COMMENT_HAS_CREATOR]-(comment211),
                        |(friend1)<-[:COMMENT_HAS_CREATOR]-(comment21111),
                        |(friend2)<-[:COMMENT_HAS_CREATOR]-(comment2111),
                        |(friend2)<-[:COMMENT_HAS_CREATOR]-(comment311),
                        |(friendfriend4)<-[:COMMENT_HAS_CREATOR]-(comment1211),
                        |(post11)<-[:REPLY_OF_POST]-(comment111),
                        |(post12)<-[:REPLY_OF_POST]-(comment121)<-[:REPLY_OF_COMMENT]-(comment1211),
                        |(post21)<-[:REPLY_OF_POST]-(comment211)<-[:REPLY_OF_COMMENT]-(comment2111)<-[:REPLY_OF_COMMENT]-(comment21111),
                        |(post31)<-[:REPLY_OF_POST]-(comment311)""".stripMargin


    def createParams = Map("friendfriend4" ->
      Map("id" -> 4L, "lastName" -> "four-ᚠさ丵פش", "firstName" -> "friendfriend"), "comment1211" ->
      Map("content" -> "C1211", "id" -> 1211L, "creationDate" -> 10L), "post21" ->
      Map("id" -> 21L, "creationDate" -> 6L, "imageFile" -> "P21 - image"), "comment121" ->
      Map("content" -> "C121", "id" -> 121L, "creationDate" -> 5L), "person0" ->
      Map("id" -> 0L, "lastName" -> "zero-ᚠさ丵פش", "firstName" -> "person"), "comment111" ->
      Map("content" -> "C111", "id" -> 111L, "creationDate" -> 12L), "post31" ->
      Map("content" -> "P31 - content", "id" -> 31L, "creationDate" -> 1L, "imageFile" -> "P31 - image"), "stranger3" ->
      Map("id" -> 3L, "lastName" -> "three-ᚠさ丵פش", "firstName" -> "stranger"), "comment311" ->
      Map("content" -> "C311", "id" -> 311L, "creationDate" -> 4L), "comment21111" ->
      Map("content" -> "C21111", "id" -> 21111L, "creationDate" -> 9L), "friend1" ->
      Map("id" -> 1L, "lastName" -> "one-ᚠさ丵פش", "firstName" -> "friend"), "comment2111" ->
      Map("content" -> "C2111", "id" -> 2111L, "creationDate" -> 8L), "post11" ->
      Map("content" -> "P11 - content", "id" -> 11L, "creationDate" -> 11L, "imageFile" -> "P11 - image"), "post12" ->
      Map("content" -> "P12 - content", "id" -> 12L, "creationDate" -> 4L, "imageFile" -> "P12 - image"), "post01" ->
      Map("content" -> "P01 - content", "id" -> 1L, "creationDate" -> 3L, "imageFile" -> "P01 - image"), "comment211" ->
      Map("content" -> "C211", "id" -> 211L, "creationDate" -> 7L), "friend2" ->
      Map("id" -> 2L, "lastName" -> "two-ᚠさ丵פش", "firstName" -> "friend"))

    val query = """MATCH (:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)<-[:POST_HAS_CREATOR|COMMENT_HAS_CREATOR]-(message)
                  |WHERE message.creationDate < {2}
                  |RETURN DISTINCT message.id AS messageId, CASE has(message.content) WHEN true THEN message.content ELSE message.imageFile END AS messageContent,
                  | message.creationDate AS messageCreationDate, friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName
                  |ORDER BY message.creationDate DESC, message.id ASC
                  |LIMIT {3}""".stripMargin

    def params = Map("1" -> 0, "2" -> 12L, "3" -> 10)

    def expectedResult = List(
      Map("personId" -> 1, "messageId" -> 11, "personLastName" -> "one-ᚠさ丵פش", "messageContent" -> "P11 - content",
        "personFirstName" -> "friend", "messageCreationDate" -> 11),
      Map("personId" -> 4, "messageId" -> 1211,
        "personLastName" -> "four-ᚠさ丵פش", "messageContent" -> "C1211", "personFirstName" -> "friendfriend",
        "messageCreationDate" -> 10),
      Map("personId" -> 1, "messageId" -> 21111, "personLastName" -> "one-ᚠさ丵פش", "messageContent" -> "C21111",
        "personFirstName" -> "friend", "messageCreationDate" -> 9),
      Map("personId" -> 2, "messageId" -> 2111,
        "personLastName" -> "two-ᚠさ丵פش", "messageContent" -> "C2111", "personFirstName" -> "friend", "messageCreationDate" -> 8),
      Map("personId" -> 1, "messageId" -> 211, "personLastName" -> "one-ᚠさ丵פش", "messageContent" -> "C211",
        "personFirstName" -> "friend", "messageCreationDate" -> 7),
      Map("personId" -> 2, "messageId" -> 21,
        "personLastName" -> "two-ᚠさ丵פش", "messageContent" -> "P21 - image", "personFirstName" -> "friend", "messageCreationDate" -> 6),
      Map("personId" -> 1, "messageId" -> 12, "personLastName" -> "one-ᚠさ丵פش", "messageContent" -> "P12 - content",
        "personFirstName" -> "friend", "messageCreationDate" -> 4),
      Map("personId" -> 2, "messageId" -> 311,
        "personLastName" -> "two-ᚠさ丵פش", "messageContent" -> "C311", "personFirstName" -> "friend", "messageCreationDate" -> 4))
  }

  object Query10 extends LdbcQuery {

    val name = "LDBC Query 10"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person0:Person {person0}),
                        | (f1:Person {f1}),
                        | (f2:Person {f2}),
                        | (ff11:Person {ff11}),
                        | (ff12:Person {ff12}),
                        | (ff21:Person {ff21}),
                        | (ff22:Person {ff22}),
                        | (ff23:Person {ff23}),
                        | (post21:Post {post21}),
                        | (post111:Post {post111}),
                        | (post112:Post {post112}),
                        | (post113:Post {post113}),
                        | (post121:Post {post121}),
                        | (post211:Post {post211}),
                        | (post212:Post {post212}),
                        | (post213:Post {post213}),
                        | (city0:City {city0}),
                        | (city1:City {city1}),
                        | (uncommonTag1:Tag {uncommonTag1}),
                        | (uncommonTag2:Tag {uncommonTag2}),
                        | (uncommonTag3:Tag {uncommonTag3}),
                        | (commonTag4:Tag {commonTag4}),
                        | (commonTag5:Tag {commonTag5}),
                        | (commonTag6:Tag {commonTag6}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(person0)-[:KNOWS]->(f1)-[:KNOWS]->(ff11),
                        |(ff11)<-[:KNOWS]-(f2)<-[:KNOWS]-(f1)-[:KNOWS]->(ff12),
                        |(person0)-[:KNOWS]->(f2)-[:KNOWS]->(ff21),
                        |(f2)-[:KNOWS]->(ff22),
                        |(f2)-[:KNOWS]->(ff23),
                        | (person0)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f1)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (f2)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff11)-[:PERSON_IS_LOCATED_IN]->(city1),
                        | (ff12)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff21)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff22)-[:PERSON_IS_LOCATED_IN]->(city0),
                        | (ff23)-[:PERSON_IS_LOCATED_IN]->(city0),
                        |(f2)<-[:POST_HAS_CREATOR]-(post21),
                        |(ff11)<-[:POST_HAS_CREATOR]-(post111),
                        |(ff11)<-[:POST_HAS_CREATOR]-(post112),
                        |(ff11)<-[:POST_HAS_CREATOR]-(post113),
                        |(ff12)<-[:POST_HAS_CREATOR]-(post121),
                        |(ff21)<-[:POST_HAS_CREATOR]-(post211),
                        |(ff21)<-[:POST_HAS_CREATOR]-(post212),
                        |(ff21)<-[:POST_HAS_CREATOR]-(post213),
                        |(person0)-[:HAS_INTEREST]->(commonTag4),
                        |(person0)-[:HAS_INTEREST]->(commonTag5),
                        |(person0)-[:HAS_INTEREST]->(commonTag6),
                        |(post21)-[:POST_HAS_TAG]->(commonTag4),
                        |(post111)-[:POST_HAS_TAG]->(uncommonTag2),
                        |(post112)-[:POST_HAS_TAG]->(uncommonTag2),
                        |(post113)-[:POST_HAS_TAG]->(commonTag5),
                        |(post113)-[:POST_HAS_TAG]->(commonTag6),
                        |(post211)-[:POST_HAS_TAG]->(uncommonTag1),
                        |(post212)-[:POST_HAS_TAG]->(uncommonTag3),
                        |(post212)-[:POST_HAS_TAG]->(commonTag4),
                        |(post213)-[:POST_HAS_TAG]->(uncommonTag3)""".stripMargin


    def createParams = Map("ff22" ->
      Map("birthday_day" -> 1, "id" -> 22L, "birthday" -> 1267398000000L, "lastName" -> "two two-ᚠさ丵פش", "gender" -> "male", "firstName" -> "friendfriend", "birthday_month" -> 2), "ff23" ->
      Map("birthday_day" -> 1, "id" -> 23L, "birthday" -> 1270072800000L, "lastName" -> "two three-ᚠさ丵פش", "gender" -> "male", "firstName" -> "friendfriend", "birthday_month" -> 3), "ff21" ->
      Map("birthday_day" -> 1, "id" -> 21L, "birthday" -> 1267398000000L, "lastName" -> "two one-ᚠさ丵פش", "gender" -> "male", "firstName" -> "friendfriend", "birthday_month" -> 2), "post21" ->
      Map("content" -> "P21", "id" -> 21L), "person0" ->
      Map("birthday_day" -> 1, "id" -> 0L, "birthday" -> 1277935200000L, "lastName" -> "zero-ᚠさ丵פش", "gender" -> "male", "firstName" -> "person", "birthday_month" -> 6), "city0" ->
      Map("name" -> "city0"), "city1" ->
      Map("name" -> "city1"), "post211" ->
      Map("content" -> "P211", "id" -> 211L), "post212" ->
      Map("content" -> "P212", "id" -> 212L), "post121" ->
      Map("content" -> "P121", "id" -> 121L), "post213" ->
      Map("content" -> "P213", "id" -> 213L), "commonTag4" ->
      Map("name" -> "common tag 4"), "uncommonTag1" ->
      Map("name" -> "uncommon tag 1"), "commonTag6" ->
      Map("name" -> "common tag 6"), "f1" ->
      Map("birthday_day" -> 1, "id" -> 1L, "birthday" -> 1267398000000L, "lastName" -> "one-ᚠさ丵פش", "gender" -> "male", "firstName" -> "friend", "birthday_month" -> 2), "commonTag5" ->
      Map("name" -> "common tag 5"), "ff12" ->
      Map("birthday_day" -> 1, "id" -> 12L, "birthday" -> 1267398000000L, "lastName" -> "one two-ᚠさ丵פش", "gender" -> "male", "firstName" -> "friendfriend", "birthday_month" -> 2), "f2" ->
      Map("birthday_day" -> 1, "id" -> 2L, "birthday" -> 1267398000000L, "lastName" -> "two-ᚠさ丵פش", "gender" -> "male", "firstName" -> "friend", "birthday_month" -> 2), "ff11" ->
      Map("birthday_day" -> 1, "id" -> 11L, "birthday" -> 1267398000000L, "lastName" -> "one one-ᚠさ丵פش", "gender" -> "female", "firstName" -> "friendfriend", "birthday_month" -> 2), "post111" ->
      Map("content" -> "P111", "id" -> 111L), "uncommonTag3" ->
      Map("name" -> "common tag 3"), "post112" ->
      Map("content" -> "P112", "id" -> 112L), "uncommonTag2" ->
      Map("name" -> "uncommon tag 2"), "post113" ->
      Map("content" -> "P113", "id" -> 113L))

    val query = """MATCH (person:Person {id:{1}})-[:KNOWS*2..2]-(friend:Person)-[:PERSON_IS_LOCATED_IN]->(city:City)
                  |WHERE ((friend.birthday_month = {2} AND friend.birthday_day >= 21) OR (friend.birthday_month = ({2}%12)+1 AND friend.birthday_day < 22)) AND not(friend=person) AND not((friend)-[:KNOWS]-(person))
                  |WITH DISTINCT friend, city, person
                  |OPTIONAL MATCH (friend)<-[:POST_HAS_CREATOR]-(post:Post)
                  |WITH friend, city, collect(post) AS posts, person
                  |WITH friend, city, length(posts) AS postCount, length([p IN posts WHERE (p)-[:POST_HAS_TAG]->(:Tag)<-[:HAS_INTEREST]-(person)]) AS commonPostCount
                  |RETURN friend.id AS personId, friend.firstName AS personFirstName, friend.lastName AS personLastName, friend.gender AS personGender, city.name AS personCityName, commonPostCount - (postCount - commonPostCount) AS commonInterestScore
                  |ORDER BY commonInterestScore DESC, personId ASC
                  |LIMIT {4}""".stripMargin

    def params = Map("1" -> 0, "2" -> 1, "4" -> 7)

    def expectedResult = List(
      Map("personId" -> 22, "personGender" -> "male", "personLastName" -> "two two-ᚠさ丵פش", "commonInterestScore" -> 0,
        "personFirstName" -> "friendfriend", "personCityName" -> "city0"),
      Map("personId" -> 11,
        "personGender" -> "female", "personLastName" -> "one one-ᚠさ丵פش", "commonInterestScore" -> -1,
        "personFirstName" -> "friendfriend", "personCityName" -> "city1"),
      Map("personId" -> 12, "personGender" -> "male", "personLastName" -> "one two-ᚠさ丵פش", "commonInterestScore" -> -1,
        "personFirstName" -> "friendfriend", "personCityName" -> "city0"),
      Map("personId" -> 21, "personGender" -> "male",
        "personLastName" -> "two one-ᚠさ丵פش", "commonInterestScore" -> -1, "personFirstName" -> "friendfriend",
        "personCityName" -> "city0"))
  }

  object Query11 extends LdbcQuery {

    val name = "LDBC Query 11"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person0:Person {person0}),
                        | (f1:Person {f1}),
                        | (f2:Person {f2}),
                        | (stranger3:Person {stranger3}),
                        | (ff11:Person {ff11}),
                        | (company0:Company {company0}),
                        | (company1:Company {company1}),
                        | (company2:Company {company2}),
                        | (country0:Country {country0}),
                        | (country1:Country {country1}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(person0)-[:KNOWS]->(f1)-[:KNOWS]->(ff11),
                        | (f1)-[:KNOWS]->(f2),
                        |(person0)-[:KNOWS]->(f2),
                        |(f1)-[:WORKS_AT {f1WorkedAtCompany0}]->(company0),
                        |(f1)-[:WORKS_AT {f1WorkedAtCompany1}]->(company1),
                        |(f2)-[:WORKS_AT {f2WorkedAtCompany2}]->(company2),
                        |(ff11)-[:WORKS_AT {ff11WorkedAtCompany0}]->(company0),
                        |(stranger3)-[:WORKS_AT {stranger3WorkedAtCompany2}]->(company2),
                        |(company0)-[:ORGANISATION_IS_LOCATED_IN]->(country0),
                        |(company1)-[:ORGANISATION_IS_LOCATED_IN]->(country1),
                        |(company2)-[:ORGANISATION_IS_LOCATED_IN]->(country0)""".stripMargin


    def createParams = Map("country0" ->
      Map("name" -> "country0"), "country1" ->
      Map("name" -> "country1"), "stranger3WorkedAtCompany2" ->
      Map("workFrom" -> 1), "person0" ->
      Map("id" -> 0L, "lastName" -> "zero-ᚠさ丵פش", "firstName" -> "person"), "company2" ->
      Map("name" -> "company two"), "f1WorkedAtCompany1" ->
      Map("workFrom" -> 4), "company1" ->
      Map("name" -> "company one"), "f1WorkedAtCompany0" ->
      Map("workFrom" -> 2), "f2WorkedAtCompany2" ->
      Map("workFrom" -> 5), "stranger3" ->
      Map("id" -> 3L, "lastName" -> "three-ᚠさ丵פش", "firstName" -> "stranger"), "company0" ->
      Map("name" -> "company zero"), "ff11WorkedAtCompany0" ->
      Map("workFrom" -> 3), "f1" ->
      Map("id" -> 1L, "lastName" -> "one-ᚠさ丵פش", "firstName" -> "friend"), "f2" ->
      Map("id" -> 2L, "lastName" -> "two-ᚠさ丵פش", "firstName" -> "friend"), "ff11" ->
      Map("id" -> 11L, "lastName" -> "one one-ᚠさ丵פش", "firstName" -> "friend friend"))

    val query = """MATCH (person:Person {id:{1}})-[:KNOWS*1..2]-(friend:Person)
                  |WHERE not(person=friend)
                  |WITH DISTINCT friend
                  |MATCH (friend)-[worksAt:WORKS_AT]->(company:Company)-[:ORGANISATION_IS_LOCATED_IN]->(:Country {name:{3}})
                  |WHERE worksAt.workFrom < {2}
                  |RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, worksAt.workFrom AS workFromYear, company.name AS companyName
                  |ORDER BY workFromYear ASC, friendId ASC, companyName DESC
                  |LIMIT {4}""".stripMargin

    def params = Map("1" -> 0, "3" -> "country0", "2" -> 5, "4" -> 4)

    def expectedResult = List(
      Map("friendLastName" -> "one-ᚠさ丵פش", "friendId" -> 1, "companyName" -> "company zero",
        "friendFirstName" -> "friend", "workFromYear" -> 2),
      Map("friendLastName" -> "one one-ᚠさ丵פش", "friendId" -> 11, "companyName" -> "company zero",
        "friendFirstName" -> "friend friend", "workFromYear" -> 3))
  }

  object Query12 extends LdbcQuery {

    val name = "LDBC Query 12"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (person0:Person {person0}),
                        | (f1:Person {f1}),
                        | (f2:Person {f2}),
                        | (f3:Person {f3}),
                        | (f4:Person {f4}),
                        | (ff11:Person {ff11}),
                        | (tc1:TagClass {tc1}),
                        | (tc11:TagClass {tc11}),
                        | (tc12:TagClass {tc12}),
                        | (tc121:TagClass {tc121}),
                        | (tc1211:TagClass {tc1211}),
                        | (tc2:TagClass {tc2}),
                        | (tc21:TagClass {tc21}),
                        | (t11:Tag {t11}),
                        | (t111:Tag {t111}),
                        | (t112:Tag {t112}),
                        | (t12111:Tag {t12111}),
                        | (t21:Tag {t21}),
                        | (t211:Tag {t211}),
                        | (p11:Post {content:'p11'}),
                        | (p111:Post {content:'p111'}),
                        | (p112:Post {content:'p112'}),
                        | (p12111:Post {content:'p12111'}),
                        | (p21:Post {content:'p21'}),
                        | (p211:Post {content:'p211'}),
                        | (c111:Comment {content:'c111'}),
                        | (c1111:Comment {content:'c1111'}),
                        | (c11111:Comment {content:'c11111'}),
                        | (c111111:Comment {content:'c111111'}),
                        | (c11112:Comment {content:'c11112'}),
                        | (c1112:Comment {content:'c1112'}),
                        | (c1121:Comment {content:'c1121'}),
                        | (c11211:Comment {content:'c11211'}),
                        | (c112111:Comment {content:'c112111'}),
                        | (c112112:Comment {content:'c112112'}),
                        | (c121111:Comment {content:'c121111'}),
                        | (c211:Comment {content:'c211'}),
                        | (c2111:Comment {content:'c2111'}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(person0)-[:KNOWS]->(f1),
                        |(person0)-[:KNOWS]->(f2),
                        |(person0)-[:KNOWS]->(f3),
                        |(person0)-[:KNOWS]->(f4),
                        |(f1)-[:KNOWS]->(ff11),
                        |(f1)<-[:COMMENT_HAS_CREATOR]-(c111111),
                        |(f1)<-[:COMMENT_HAS_CREATOR]-(c1111),
                        |(f1)<-[:COMMENT_HAS_CREATOR]-(c11211),
                        |(f1)<-[:COMMENT_HAS_CREATOR]-(c121111),
                        |(f2)<-[:COMMENT_HAS_CREATOR]-(c1112),
                        |(f2)<-[:COMMENT_HAS_CREATOR]-(c112111),
                        |(f3)<-[:COMMENT_HAS_CREATOR]-(c112112),
                        |(f3)<-[:COMMENT_HAS_CREATOR]-(c111),
                        |(f3)<-[:COMMENT_HAS_CREATOR]-(c211),
                        |(f3)<-[:COMMENT_HAS_CREATOR]-(c2111),
                        |(ff11)<-[:COMMENT_HAS_CREATOR]-(c11112),
                        |(ff11)<-[:COMMENT_HAS_CREATOR]-(c11111),
                        |(ff11)<-[:COMMENT_HAS_CREATOR]-(c1121),
                        |(c1111)<-[:REPLY_OF_COMMENT]-(c11111),
                        |(c1111)<-[:REPLY_OF_COMMENT]-(c11112),
                        |(c11111)<-[:REPLY_OF_COMMENT]-(c111111),
                        |(c1121)<-[:REPLY_OF_COMMENT]-(c11211),
                        |(c11211)<-[:REPLY_OF_COMMENT]-(c112111),
                        |(c11211)<-[:REPLY_OF_COMMENT]-(c112112),
                        |(p11)<-[:REPLY_OF_POST]-(c111),
                        |(p111)<-[:REPLY_OF_POST]-(c1111),
                        |(p111)<-[:REPLY_OF_POST]-(c1112),
                        |(p112)<-[:REPLY_OF_POST]-(c1121),
                        |(p12111)<-[:REPLY_OF_POST]-(c121111),
                        |(p21)<-[:REPLY_OF_POST]-(c211),
                        |(p211)<-[:REPLY_OF_POST]-(c2111),
                        |(p11)-[:POST_HAS_TAG]->(t11),
                        |(p11)-[:POST_HAS_TAG]->(t12111),
                        |(p111)-[:POST_HAS_TAG]->(t111),
                        |(p112)-[:POST_HAS_TAG]->(t112),
                        |(p12111)-[:POST_HAS_TAG]->(t12111),
                        |(p12111)-[:POST_HAS_TAG]->(t21),
                        |(p21)-[:POST_HAS_TAG]->(t21),
                        |(p211)-[:POST_HAS_TAG]->(t211),
                        |(c1111)-[:COMMENT_HAS_TAG]->(t112),
                        |(c1121)-[:COMMENT_HAS_TAG]->(t11),
                        |(c11211)-[:COMMENT_HAS_TAG]->(t12111),
                        |(tc1)<-[:HAS_TYPE]-(t11),
                        |(tc11)<-[:HAS_TYPE]-(t111),
                        |(tc11)<-[:HAS_TYPE]-(t112),
                        |(tc1211)<-[:HAS_TYPE]-(t12111),
                        |(tc2)<-[:HAS_TYPE]-(t21),
                        |(tc21)<-[:HAS_TYPE]-(t211),
                        |(tc11)-[:IS_SUBCLASS_OF]->(tc1),
                        |(tc12)-[:IS_SUBCLASS_OF]->(tc1),
                        |(tc121)-[:IS_SUBCLASS_OF]->(tc12),
                        |(tc1211)-[:IS_SUBCLASS_OF]->(tc121),
                        |(tc21)-[:IS_SUBCLASS_OF]->(tc2)""".stripMargin


    def createParams = Map("tc1211" ->
      Map("name" -> "1211"), "t21" ->
      Map("name" -> "tag21-ᚠさ丵פش"), "tc12" ->
      Map("name" -> "12"), "t11" ->
      Map("name" -> "tag11-ᚠさ丵פش"), "person0" ->
      Map("id" -> 0L, "lastName" -> "0", "firstName" -> "person"), "t12111" ->
      Map("name" -> "tag12111-ᚠさ丵פش"), "t211" ->
      Map("name" -> "tag211-ᚠさ丵פش"), "tc121" ->
      Map("name" -> "121"), "tc21" ->
      Map("name" -> "21"), "t111" ->
      Map("name" -> "tag111-ᚠさ丵פش"), "tc11" ->
      Map("name" -> "11"), "f1" ->
      Map("id" -> 1L, "lastName" -> "1", "firstName" -> "f"), "t112" ->
      Map("name" -> "tag112-ᚠさ丵פش"), "tc2" ->
      Map("name" -> "2"), "f3" ->
      Map("id" -> 3L, "lastName" -> "3", "firstName" -> "f"), "tc1" ->
      Map("name" -> "1"), "f2" ->
      Map("id" -> 2L, "lastName" -> "2", "firstName" -> "f"), "ff11" ->
      Map("id" -> 11L, "lastName" -> "11", "firstName" -> "ff"), "f4" ->
      Map("id" -> 4L, "lastName" -> "4", "firstName" -> "f"))

    val query = """MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)
                  |OPTIONAL MATCH (friend)<-[:COMMENT_HAS_CREATOR]-(comment:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
                  |WHERE tagClass.name = {2} OR baseTagClass.name = {2}
                  |RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, collect(DISTINCT tag.name) AS tagNames, count(DISTINCT comment) AS count
                  |ORDER BY count DESC, friendId ASC
                  |LIMIT {3}""".stripMargin

    def params = Map("1" -> 0, "2" -> 1, "3" -> 10)

    def expectedResult = List(
      Map("friendLastName" -> "1", "tagNames" -> Seq.empty, "friendId" -> 1, "count" -> 0, "friendFirstName" -> "f"),
      Map("friendLastName" -> "2", "tagNames" -> Seq.empty, "friendId" -> 2, "count" -> 0, "friendFirstName" -> "f"),
      Map("friendLastName" -> "3", "tagNames" -> Seq.empty, "friendId" -> 3, "count" -> 0, "friendFirstName" -> "f"),
      Map("friendLastName" -> "4", "tagNames" -> Seq.empty, "friendId" -> 4, "count" -> 0, "friendFirstName" -> "f"))
  }

  object Query13 extends LdbcQuery {

    val name = "LDBC Query 13"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        | (p0:Person {id:0}),
                        | (p1:Person {id:1}),
                        | (p2:Person {id:2}),
                        | (p3:Person {id:3}),
                        | (p4:Person {id:4}),
                        | (p5:Person {id:5}),
                        | (p6:Person {id:6}),
                        | (p7:Person {id:7}),
                        | (p8:Person {id:8}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(p0)-[:KNOWS]->(p1),
                        |(p1)-[:KNOWS]->(p3),
                        |(p1)<-[:KNOWS]-(p2),
                        |(p3)-[:KNOWS]->(p2),
                        |(p2)<-[:KNOWS]-(p4),
                        |(p4)-[:KNOWS]->(p7),
                        |(p4)-[:KNOWS]->(p6),
                        |(p6)<-[:KNOWS]-(p5)""".stripMargin


    def createParams = Map.empty

    val query = """MATCH (person1:Person {id:{1}}), (person2:Person {id:{2}})
                  |OPTIONAL MATCH path = shortestPath((person1)-[:KNOWS*0..100]-(person2))
                  |RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS pathLength""".stripMargin

    def params = Map("1" -> 0, "2" -> 5)

    def expectedResult = List(Map("pathLength" -> 5))
  }

  object Query14 extends LdbcQuery {

    val name = "LDBC Query 14"

    val createQuery = """CREATE
                        |
                        |// --- NODES ---
                        |
                        |(p0:Person {id:0}),
                        |(p1:Person {id:1}),
                        |(p2:Person {id:2}),
                        |(p3:Person {id:3}),
                        |(p4:Person {id:4}),
                        |(p5:Person {id:5}),
                        |(p6:Person {id:6}),
                        |(p7:Person {id:7}),
                        |(p8:Person {id:8}),
                        |(p9:Person {id:9}),
                        |(p0Post1:Post {id:0}),
                        |(p1Post1:Post {id:1}),
                        |(p3Post1:Post {id:2}),
                        |(p5Post1:Post {id:3}),
                        |(p6Post1:Post {id:4}),
                        |(p7Post1:Post {id:5}),
                        |(p0Comment1:Comment {id:6}),
                        |(p1Comment1:Comment {id:7}),
                        |(p1Comment2:Comment {id:8}),
                        |(p4Comment1:Comment {id:9}),
                        |(p4Comment2:Comment {id:10}),
                        |(p5Comment1:Comment {id:11}),
                        |(p5Comment2:Comment {id:12}),
                        |(p7Comment1:Comment {id:13}),
                        |(p8Comment1:Comment {id:14}),
                        |(p8Comment2:Comment {id:15}),
                        |
                        |// --- RELATIONSHIPS ---
                        |
                        |(p0)-[:KNOWS]->(p1),
                        |(p1)-[:KNOWS]->(p3),
                        |(p1)<-[:KNOWS]-(p2),
                        |(p1)<-[:KNOWS]-(p7),
                        |(p3)-[:KNOWS]->(p2),
                        |(p2)<-[:KNOWS]-(p4),
                        |(p4)-[:KNOWS]->(p7),
                        |(p4)-[:KNOWS]->(p8),
                        |(p4)-[:KNOWS]->(p6),
                        |(p6)<-[:KNOWS]-(p5),
                        |(p8)<-[:KNOWS]-(p5),
                        |(p0)<-[:POST_HAS_CREATOR]-(p0Post1),
                        |(p1)<-[:POST_HAS_CREATOR]-(p1Post1),
                        |(p3)<-[:POST_HAS_CREATOR]-(p3Post1),
                        |(p5)<-[:POST_HAS_CREATOR]-(p5Post1),
                        |(p6)<-[:POST_HAS_CREATOR]-(p6Post1),
                        |(p7)<-[:POST_HAS_CREATOR]-(p7Post1),
                        |(p0)<-[:COMMENT_HAS_CREATOR]-(p0Comment1),
                        |(p1)<-[:COMMENT_HAS_CREATOR]-(p1Comment1),
                        |(p1)<-[:COMMENT_HAS_CREATOR]-(p1Comment2),
                        |(p4)<-[:COMMENT_HAS_CREATOR]-(p4Comment1),
                        |(p4)<-[:COMMENT_HAS_CREATOR]-(p4Comment2),
                        |(p5)<-[:COMMENT_HAS_CREATOR]-(p5Comment1),
                        |(p5)<-[:COMMENT_HAS_CREATOR]-(p5Comment2),
                        |(p7)<-[:COMMENT_HAS_CREATOR]-(p7Comment1),
                        |(p8)<-[:COMMENT_HAS_CREATOR]-(p8Comment1),
                        |(p8)<-[:COMMENT_HAS_CREATOR]-(p8Comment2),
                        |(p1Post1)<-[:REPLY_OF_POST]-(p0Comment1),
                        |(p0Post1)<-[:REPLY_OF_POST]-(p1Comment1),
                        |(p0Post1)<-[:REPLY_OF_POST]-(p1Comment2),
                        |(p3Post1)<-[:REPLY_OF_POST]-(p4Comment1),
                        |(p7Post1)<-[:REPLY_OF_POST]-(p4Comment2),
                        |(p5Post1)<-[:REPLY_OF_POST]-(p5Comment1),
                        |(p6Post1)<-[:REPLY_OF_POST]-(p8Comment1),
                        |(p7Comment1)-[:REPLY_OF_COMMENT]->(p4Comment2),
                        |(p8Comment2)-[:REPLY_OF_COMMENT]->(p4Comment1),
                        |(p5Comment2)-[:REPLY_OF_COMMENT]->(p8Comment2)""".stripMargin


    def createParams = Map.empty

    val query = """MATCH path = allShortestPaths((person1:Person {id:{1}})-[:KNOWS*0..100]-(person2:Person {id:{2}}))
                  |WITH nodes(path) AS pathNodes
                  |RETURN
                  | extract(n IN pathNodes | n.id) AS pathNodeIds,
                  | reduce(weight=0.0, idx IN range(1,size(pathNodes)-1) |
                  |    extract(prev IN [pathNodes[idx-1]] |
                  |        extract(curr IN [pathNodes[idx]] |
                  |            weight +
                  |            length((curr)<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->(prev))*1.0 +
                  |            length((prev)<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->(curr))*1.0 +
                  |            length((prev)-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_COMMENT]-(:Comment)-[:COMMENT_HAS_CREATOR]-(curr))*0.5
                  |        )
                  |    )[0][0]
                  | ) AS weight
                  |ORDER BY weight DESC""".stripMargin

    def params = Map("1" -> 0, "2" -> 5)

    def expectedResult = List(
      Map("weight" -> 5.5, "pathNodeIds" -> List(0, 1, 7, 4, 8, 5)),
      Map("weight" -> 4.5, "pathNodeIds" -> List(0, 1, 7, 4, 6, 5)),
      Map("weight" -> 4.0, "pathNodeIds" -> List(0, 1, 2, 4, 8, 5)),
      Map("weight" -> 3.0, "pathNodeIds" -> List(0, 1, 2, 4, 6, 5)))
  }

  object Query14_v2 extends LdbcQuery {

    val name = "LDBC Query 14 v2"

    val createQuery = Query14.createQuery

    def createParams = Map.empty

    val query = """MATCH path = allShortestPaths((person1:Person {id:{1}})-[:KNOWS*0..]-(person2:Person {id:{2}}))
                  |RETURN
                  |extract(n IN nodes(path) | n.id) AS pathNodeIds,
                  |reduce(weight=0.0, r IN rels(path) |
                  |           weight +
                  |           length(()-[r]->()<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->()-[r]->())*1.0 +
                  |           length(()<-[r]-()<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->()<-[r]-())*1.0 +
                  |           length(()<-[r]-()-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_COMMENT]-(:Comment)-[:COMMENT_HAS_CREATOR]-()<-[r]-())*0.5
                  |) AS weight
                  |ORDER BY weight DESC""".stripMargin

    def params = Map("1" -> 0, "2" -> 5)

    def expectedResult = List(
      Map("weight" -> 5.5, "pathNodeIds" -> List(0, 1, 7, 4, 8, 5)),
      Map("weight" -> 4.5, "pathNodeIds" -> List(0, 1, 7, 4, 6, 5)),
      Map("weight" -> 4.0, "pathNodeIds" -> List(0, 1, 2, 4, 8, 5)),
      Map("weight" -> 3.0, "pathNodeIds" -> List(0, 1, 2, 4, 6, 5)))
  }

  val LDBC_QUERIES = Seq(
    Query1,
    Query2,
    Query3,
    Query4,
    Query5,
    Query6,
    Query7,
    Query8,
    Query9,
    Query10,
    Query11,
    Query12,
    Query13,
    Query14,
    Query14_v2)
}
