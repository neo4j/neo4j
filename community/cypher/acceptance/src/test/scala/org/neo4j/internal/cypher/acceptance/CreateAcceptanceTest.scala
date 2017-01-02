/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_0.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport, SyntaxException}
import org.neo4j.graphdb.{Direction, Relationship, RelationshipType}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport
  with CreateTempFileTestSupport {

  test("create a single node") {
    val result = updateWithBothPlannersAndCompatibilityMode("create ()")
    assertStats(result, nodesCreated = 1)
    // then
    result.toList shouldBe empty
  }

  test("create a single node with single label") {
    val result = updateWithBothPlannersAndCompatibilityMode("create (:A)")
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
    // then
    result.toList shouldBe empty
  }

  test("create a single node with multiple labels") {
    val result = updateWithBothPlannersAndCompatibilityMode("create (n:A:B:C:D)")
    assertStats(result, nodesCreated = 1, labelsAdded = 4)
    // then
    result.toList shouldBe empty
  }

  test("combine match and create") {
    createNode()
    createNode()
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) create ()")
    assertStats(result, nodesCreated = 2)
    // then
    result.toList shouldBe empty
  }

  test("combine match, with, and create") {
    createNode()
    createNode()
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) create (n1) with * match(p) create (n2)")
    assertStats(result, nodesCreated = 10)
    // then
    result.toList shouldBe empty
  }


  test("should not see updates created by itself") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("match (n) create ()")
    assertStats(result, nodesCreated = 1)
  }

  test("create a single node with properties") {
    val result = updateWithBothPlannersAndCompatibilityMode("create (n {prop: 'foo'}) return n.prop as p")
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    // then
    result.toList should equal(List(Map("p" -> "foo")))
  }

  test("using an undirected relationship pattern should fail on create") {
      intercept[SyntaxException](executeScalar[Relationship]("create (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r"))
  }

  test("create node using null properties should just ignore those properties") {
    // when
    val result = updateWithBothPlannersAndCompatibilityMode("create (n {id: 12, property: null}) return n.id as id")
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)

    // then
   result.toList should equal(List(Map("id" -> 12)))
  }

  test("create relationship using null properties should just ignore those properties") {
    // when
    val result = updateWithBothPlannersAndCompatibilityMode("create ()-[r:X {id: 12, property: null}]->() return r.id")
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1)

    // then
    result.toList should equal(List(Map("r.id" -> 12)))
  }

  test("create simple pattern") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a)-[r:R]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create simple loop") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (root: R)-[:LINK]->(root)")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1, labelsAdded = 1)
  }

  test("create simple loop from match") {
    createLabeledNode("R")
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (root:R) CREATE (root)-[:LINK]->(root)")

    assertStats(result, relationshipsCreated = 1)
  }

  test("create both nodes and relationships") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a), (b), (a)-[r:R]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create relationship with property") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a)-[r:R {prop: 42}]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  test("creates relationship in correct direction") {
    import scala.collection.JavaConverters._
    val start = createLabeledNode("Y")
    val end = createLabeledNode("X")

    val typ = "TYPE"
    val result = updateWithBothPlannersAndCompatibilityMode(s"MATCH (x:X), (y:Y) CREATE (x)<-[:$typ]-(y)")

    assertStats(result, relationshipsCreated = 1)
    graph.inTx {
      start.getRelationships(RelationshipType.withName(typ), Direction.OUTGOING).asScala should have size 1
      end.getRelationships(RelationshipType.withName(typ), Direction.INCOMING).asScala should have size 1
    }
  }

  test("creates one node, matches one and create relationship") {
    import scala.collection.JavaConverters._
    val start = createLabeledNode("Start")

    val typ = "TYPE"
    val result = updateWithBothPlannersAndCompatibilityMode(s"MATCH (x:Start) CREATE (x)-[:$typ]->(y:End)")

    assertStats(result, nodesCreated = 1, labelsAdded = 1, relationshipsCreated = 1)
    graph.inTx {
      start.getRelationships(RelationshipType.withName(typ), Direction.OUTGOING).asScala should have size 1
    }
  }

  test("single create after with") {
    //given
    createNode()
    createNode()

    //when
    val query = "MATCH (n) CREATE() WITH * CREATE ()"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    //then
    assertStats(result, nodesCreated = 4)
    result should not(use("Apply"))
  }
  test("create relationship with reversed direction") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a)<-[r1:R]-(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)

    executeWithAllPlannersAndCompatibilityMode("MATCH (a)<-[r1:R]-(b) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a)-[r1:R]->(b)-[r2:R]->(c)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2)

    executeWithAllPlannersAndCompatibilityMode("MATCH (a)-[r1:R]->(b)-[r2:R]->(c) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and reversed direction") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a)<-[r1:R]-(b)<-[r2:R]-(c)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2)

    executeWithAllPlannersAndCompatibilityMode("MATCH (a)<-[r1:R]-(b)<-[r2:R]-(c) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and changing directions") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a:A)-[r1:R]->(b:B)<-[r2:R]-(c:C)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, labelsAdded = 3)

    executeWithAllPlannersAndCompatibilityMode("MATCH (a:A)-[r1:R]->(b:B)<-[r2:R]-(c:C) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and changing directions 2") {
    val result = updateWithBothPlannersAndCompatibilityMode("CYPHER planner=rule CREATE (a:A)<-[r1:R]-(b:B)-[r2:R]->(c:C)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, labelsAdded = 3)

    executeWithAllPlannersAndCompatibilityMode("MATCH (a:A)<-[r1:R]-(b:B)-[r2:R]->(c:C) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and varying directions and types") {
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d)")

    assertStats(result, nodesCreated = 4, relationshipsCreated = 3)

    executeWithAllPlannersAndCompatibilityMode("MATCH (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d) RETURN *").toList should have size 1
  }

  test("should be possible to generate the movie graph") {
    val query = """
                  |CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})
                  |CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})
                  |CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})
                  |CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})
                  |CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})
                  |CREATE (AndyW:Person {name:'Andy Wachowski', born:1967})
                  |CREATE (LanaW:Person {name:'Lana Wachowski', born:1965})
                  |CREATE (JoelS:Person {name:'Joel Silver', born:1952})
                  |CREATE
                  |  (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix),
                  |  (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix),
                  |  (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix),
                  |  (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix),
                  |  (AndyW)-[:DIRECTED]->(TheMatrix),
                  |  (LanaW)-[:DIRECTED]->(TheMatrix),
                  |  (JoelS)-[:PRODUCED]->(TheMatrix)
                  |
                  |CREATE (Emil:Person {name:"Emil Eifrem", born:1978})
                  |CREATE (Emil)-[:ACTED_IN {roles:["Emil"]}]->(TheMatrix)
                  |
                  |CREATE (TheMatrixReloaded:Movie {title:'The Matrix Reloaded', released:2003, tagline:'Free your mind'})
                  |CREATE
                  |  (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrixReloaded),
                  |  (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrixReloaded),
                  |  (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrixReloaded),
                  |  (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrixReloaded),
                  |  (AndyW)-[:DIRECTED]->(TheMatrixReloaded),
                  |  (LanaW)-[:DIRECTED]->(TheMatrixReloaded),
                  |  (JoelS)-[:PRODUCED]->(TheMatrixReloaded)
                  |
                  |CREATE (TheMatrixRevolutions:Movie {title:'The Matrix Revolutions', released:2003, tagline:'Everything that has a beginning has an end'})
                  |CREATE
                  |  (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrixRevolutions),
                  |  (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrixRevolutions),
                  |  (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrixRevolutions),
                  |  (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrixRevolutions),
                  |  (AndyW)-[:DIRECTED]->(TheMatrixRevolutions),
                  |  (LanaW)-[:DIRECTED]->(TheMatrixRevolutions),
                  |  (JoelS)-[:PRODUCED]->(TheMatrixRevolutions)
                  |
                  |CREATE (TheDevilsAdvocate:Movie {title:"The Devil's Advocate", released:1997, tagline:'Evil has its winning ways'})
                  |CREATE (Charlize:Person {name:'Charlize Theron', born:1975})
                  |CREATE (Al:Person {name:'Al Pacino', born:1940})
                  |CREATE (Taylor:Person {name:'Taylor Hackford', born:1944})
                  |CREATE
                  |  (Keanu)-[:ACTED_IN {roles:['Kevin Lomax']}]->(TheDevilsAdvocate),
                  |  (Charlize)-[:ACTED_IN {roles:['Mary Ann Lomax']}]->(TheDevilsAdvocate),
                  |  (Al)-[:ACTED_IN {roles:['John Milton']}]->(TheDevilsAdvocate),
                  |  (Taylor)-[:DIRECTED]->(TheDevilsAdvocate)
                  |
                  |CREATE (AFewGoodMen:Movie {title:"A Few Good Men", released:1992, tagline:"In the heart of the nation's capital, in a courthouse of the U.S. government, one man will stop at nothing to keep his honor, and one will stop at nothing to find the truth."})
                  |CREATE (TomC:Person {name:'Tom Cruise', born:1962})
                  |CREATE (JackN:Person {name:'Jack Nicholson', born:1937})
                  |CREATE (DemiM:Person {name:'Demi Moore', born:1962})
                  |CREATE (KevinB:Person {name:'Kevin Bacon', born:1958})
                  |CREATE (KieferS:Person {name:'Kiefer Sutherland', born:1966})
                  |CREATE (NoahW:Person {name:'Noah Wyle', born:1971})
                  |CREATE (CubaG:Person {name:'Cuba Gooding Jr.', born:1968})
                  |CREATE (KevinP:Person {name:'Kevin Pollak', born:1957})
                  |CREATE (JTW:Person {name:'J.T. Walsh', born:1943})
                  |CREATE (JamesM:Person {name:'James Marshall', born:1967})
                  |CREATE (ChristopherG:Person {name:'Christopher Guest', born:1948})
                  |CREATE (RobR:Person {name:'Rob Reiner', born:1947})
                  |CREATE (AaronS:Person {name:'Aaron Sorkin', born:1961})
                  |CREATE
                  |  (TomC)-[:ACTED_IN {roles:['Lt. Daniel Kaffee']}]->(AFewGoodMen),
                  |  (JackN)-[:ACTED_IN {roles:['Col. Nathan R. Jessup']}]->(AFewGoodMen),
                  |  (DemiM)-[:ACTED_IN {roles:['Lt. Cdr. JoAnne Galloway']}]->(AFewGoodMen),
                  |  (KevinB)-[:ACTED_IN {roles:['Capt. Jack Ross']}]->(AFewGoodMen),
                  |  (KieferS)-[:ACTED_IN {roles:['Lt. Jonathan Kendrick']}]->(AFewGoodMen),
                  |  (NoahW)-[:ACTED_IN {roles:['Cpl. Jeffrey Barnes']}]->(AFewGoodMen),
                  |  (CubaG)-[:ACTED_IN {roles:['Cpl. Carl Hammaker']}]->(AFewGoodMen),
                  |  (KevinP)-[:ACTED_IN {roles:['Lt. Sam Weinberg']}]->(AFewGoodMen),
                  |  (JTW)-[:ACTED_IN {roles:['Lt. Col. Matthew Andrew Markinson']}]->(AFewGoodMen),
                  |  (JamesM)-[:ACTED_IN {roles:['Pfc. Louden Downey']}]->(AFewGoodMen),
                  |  (ChristopherG)-[:ACTED_IN {roles:['Dr. Stone']}]->(AFewGoodMen),
                  |  (AaronS)-[:ACTED_IN {roles:['Man in Bar']}]->(AFewGoodMen),
                  |  (RobR)-[:DIRECTED]->(AFewGoodMen),
                  |  (AaronS)-[:WROTE]->(AFewGoodMen)
                  |
                  |CREATE (TopGun:Movie {title:"Top Gun", released:1986, tagline:'I feel the need, the need for speed.'})
                  |CREATE (KellyM:Person {name:'Kelly McGillis', born:1957})
                  |CREATE (ValK:Person {name:'Val Kilmer', born:1959})
                  |CREATE (AnthonyE:Person {name:'Anthony Edwards', born:1962})
                  |CREATE (TomS:Person {name:'Tom Skerritt', born:1933})
                  |CREATE (MegR:Person {name:'Meg Ryan', born:1961})
                  |CREATE (TonyS:Person {name:'Tony Scott', born:1944})
                  |CREATE (JimC:Person {name:'Jim Cash', born:1941})
                  |CREATE
                  |  (TomC)-[:ACTED_IN {roles:['Maverick']}]->(TopGun),
                  |  (KellyM)-[:ACTED_IN {roles:['Charlie']}]->(TopGun),
                  |  (ValK)-[:ACTED_IN {roles:['Iceman']}]->(TopGun),
                  |  (AnthonyE)-[:ACTED_IN {roles:['Goose']}]->(TopGun),
                  |  (TomS)-[:ACTED_IN {roles:['Viper']}]->(TopGun),
                  |  (MegR)-[:ACTED_IN {roles:['Carole']}]->(TopGun),
                  |  (TonyS)-[:DIRECTED]->(TopGun),
                  |  (JimC)-[:WROTE]->(TopGun)
                  |
                  |CREATE (JerryMaguire:Movie {title:'Jerry Maguire', released:2000, tagline:'The rest of his life begins now.'})
                  |CREATE (ReneeZ:Person {name:'Renee Zellweger', born:1969})
                  |CREATE (KellyP:Person {name:'Kelly Preston', born:1962})
                  |CREATE (JerryO:Person {name:"Jerry O'Connell", born:1974})
                  |CREATE (JayM:Person {name:'Jay Mohr', born:1970})
                  |CREATE (BonnieH:Person {name:'Bonnie Hunt', born:1961})
                  |CREATE (ReginaK:Person {name:'Regina King', born:1971})
                  |CREATE (JonathanL:Person {name:'Jonathan Lipnicki', born:1996})
                  |CREATE (CameronC:Person {name:'Cameron Crowe', born:1957})
                  |CREATE
                  |  (TomC)-[:ACTED_IN {roles:['Jerry Maguire']}]->(JerryMaguire),
                  |  (CubaG)-[:ACTED_IN {roles:['Rod Tidwell']}]->(JerryMaguire),
                  |  (ReneeZ)-[:ACTED_IN {roles:['Dorothy Boyd']}]->(JerryMaguire),
                  |  (KellyP)-[:ACTED_IN {roles:['Avery Bishop']}]->(JerryMaguire),
                  |  (JerryO)-[:ACTED_IN {roles:['Frank Cushman']}]->(JerryMaguire),
                  |  (JayM)-[:ACTED_IN {roles:['Bob Sugar']}]->(JerryMaguire),
                  |  (BonnieH)-[:ACTED_IN {roles:['Laurel Boyd']}]->(JerryMaguire),
                  |  (ReginaK)-[:ACTED_IN {roles:['Marcee Tidwell']}]->(JerryMaguire),
                  |  (JonathanL)-[:ACTED_IN {roles:['Ray Boyd']}]->(JerryMaguire),
                  |  (CameronC)-[:DIRECTED]->(JerryMaguire),
                  |  (CameronC)-[:PRODUCED]->(JerryMaguire),
                  |  (CameronC)-[:WROTE]->(JerryMaguire)
                  |
                  |CREATE (StandByMe:Movie {title:"Stand By Me", released:1986, tagline:"For some, it's the last real taste of innocence, and the first real taste of life. But for everyone, it's the time that memories are made of."})
                  |CREATE (RiverP:Person {name:'River Phoenix', born:1970})
                  |CREATE (CoreyF:Person {name:'Corey Feldman', born:1971})
                  |CREATE (WilW:Person {name:'Wil Wheaton', born:1972})
                  |CREATE (JohnC:Person {name:'John Cusack', born:1966})
                  |CREATE (MarshallB:Person {name:'Marshall Bell', born:1942})
                  |CREATE
                  |  (WilW)-[:ACTED_IN {roles:['Gordie Lachance']}]->(StandByMe),
                  |  (RiverP)-[:ACTED_IN {roles:['Chris Chambers']}]->(StandByMe),
                  |  (JerryO)-[:ACTED_IN {roles:['Vern Tessio']}]->(StandByMe),
                  |  (CoreyF)-[:ACTED_IN {roles:['Teddy Duchamp']}]->(StandByMe),
                  |  (JohnC)-[:ACTED_IN {roles:['Denny Lachance']}]->(StandByMe),
                  |  (KieferS)-[:ACTED_IN {roles:['Ace Merrill']}]->(StandByMe),
                  |  (MarshallB)-[:ACTED_IN {roles:['Mr. Lachance']}]->(StandByMe),
                  |  (RobR)-[:DIRECTED]->(StandByMe)
                  |
                  |CREATE (AsGoodAsItGets:Movie {title:'As Good as It Gets', released:1997, tagline:'A comedy from the heart that goes for the throat.'})
                  |CREATE (HelenH:Person {name:'Helen Hunt', born:1963})
                  |CREATE (GregK:Person {name:'Greg Kinnear', born:1963})
                  |CREATE (JamesB:Person {name:'James L. Brooks', born:1940})
                  |CREATE
                  |  (JackN)-[:ACTED_IN {roles:['Melvin Udall']}]->(AsGoodAsItGets),
                  |  (HelenH)-[:ACTED_IN {roles:['Carol Connelly']}]->(AsGoodAsItGets),
                  |  (GregK)-[:ACTED_IN {roles:['Simon Bishop']}]->(AsGoodAsItGets),
                  |  (CubaG)-[:ACTED_IN {roles:['Frank Sachs']}]->(AsGoodAsItGets),
                  |  (JamesB)-[:DIRECTED]->(AsGoodAsItGets)
                  |
                  |CREATE (WhatDreamsMayCome:Movie {title:'What Dreams May Come', released:1998, tagline:'After life there is more. The end is just the beginning.'})
                  |CREATE (AnnabellaS:Person {name:'Annabella Sciorra', born:1960})
                  |CREATE (MaxS:Person {name:'Max von Sydow', born:1929})
                  |CREATE (WernerH:Person {name:'Werner Herzog', born:1942})
                  |CREATE (Robin:Person {name:'Robin Williams', born:1951})
                  |CREATE (VincentW:Person {name:'Vincent Ward', born:1956})
                  |CREATE
                  |  (Robin)-[:ACTED_IN {roles:['Chris Nielsen']}]->(WhatDreamsMayCome),
                  |  (CubaG)-[:ACTED_IN {roles:['Albert Lewis']}]->(WhatDreamsMayCome),
                  |  (AnnabellaS)-[:ACTED_IN {roles:['Annie Collins-Nielsen']}]->(WhatDreamsMayCome),
                  |  (MaxS)-[:ACTED_IN {roles:['The Tracker']}]->(WhatDreamsMayCome),
                  |  (WernerH)-[:ACTED_IN {roles:['The Face']}]->(WhatDreamsMayCome),
                  |  (VincentW)-[:DIRECTED]->(WhatDreamsMayCome)
                  |
                  |CREATE (SnowFallingonCedars:Movie {title:'Snow Falling on Cedars', released:1999, tagline:'First loves last. Forever.'})
                  |CREATE (EthanH:Person {name:'Ethan Hawke', born:1970})
                  |CREATE (RickY:Person {name:'Rick Yune', born:1971})
                  |CREATE (JamesC:Person {name:'James Cromwell', born:1940})
                  |CREATE (ScottH:Person {name:'Scott Hicks', born:1953})
                  |CREATE
                  |  (EthanH)-[:ACTED_IN {roles:['Ishmael Chambers']}]->(SnowFallingonCedars),
                  |  (RickY)-[:ACTED_IN {roles:['Kazuo Miyamoto']}]->(SnowFallingonCedars),
                  |  (MaxS)-[:ACTED_IN {roles:['Nels Gudmundsson']}]->(SnowFallingonCedars),
                  |  (JamesC)-[:ACTED_IN {roles:['Judge Fielding']}]->(SnowFallingonCedars),
                  |  (ScottH)-[:DIRECTED]->(SnowFallingonCedars)
                  |
                  |CREATE (YouveGotMail:Movie {title:"You've Got Mail", released:1998, tagline:'At odds in life... in love on-line.'})
                  |CREATE (ParkerP:Person {name:'Parker Posey', born:1968})
                  |CREATE (DaveC:Person {name:'Dave Chappelle', born:1973})
                  |CREATE (SteveZ:Person {name:'Steve Zahn', born:1967})
                  |CREATE (TomH:Person {name:'Tom Hanks', born:1956})
                  |CREATE (NoraE:Person {name:'Nora Ephron', born:1941})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Joe Fox']}]->(YouveGotMail),
                  |  (MegR)-[:ACTED_IN {roles:['Kathleen Kelly']}]->(YouveGotMail),
                  |  (GregK)-[:ACTED_IN {roles:['Frank Navasky']}]->(YouveGotMail),
                  |  (ParkerP)-[:ACTED_IN {roles:['Patricia Eden']}]->(YouveGotMail),
                  |  (DaveC)-[:ACTED_IN {roles:['Kevin Jackson']}]->(YouveGotMail),
                  |  (SteveZ)-[:ACTED_IN {roles:['George Pappas']}]->(YouveGotMail),
                  |  (NoraE)-[:DIRECTED]->(YouveGotMail)
                  |
                  |CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, tagline:'What if someone you never met, someone you never saw, someone you never knew was the only someone for you?'})
                  |CREATE (RitaW:Person {name:'Rita Wilson', born:1956})
                  |CREATE (BillPull:Person {name:'Bill Pullman', born:1953})
                  |CREATE (VictorG:Person {name:'Victor Garber', born:1949})
                  |CREATE (RosieO:Person {name:"Rosie O'Donnell", born:1962})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Sam Baldwin']}]->(SleeplessInSeattle),
                  |  (MegR)-[:ACTED_IN {roles:['Annie Reed']}]->(SleeplessInSeattle),
                  |  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),
                  |  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),
                  |  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),
                  |  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),
                  |  (NoraE)-[:DIRECTED]->(SleeplessInSeattle)
                  |
                  |CREATE (JoeVersustheVolcano:Movie {title:'Joe Versus the Volcano', released:1990, tagline:'A story of love, lava and burning desire.'})
                  |CREATE (JohnS:Person {name:'John Patrick Stanley', born:1950})
                  |CREATE (Nathan:Person {name:'Nathan Lane', born:1956})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Joe Banks']}]->(JoeVersustheVolcano),
                  |  (MegR)-[:ACTED_IN {roles:['DeDe', 'Angelica Graynamore', 'Patricia Graynamore']}]->(JoeVersustheVolcano),
                  |  (Nathan)-[:ACTED_IN {roles:['Baw']}]->(JoeVersustheVolcano),
                  |  (JohnS)-[:DIRECTED]->(JoeVersustheVolcano)
                  |
                  |CREATE (WhenHarryMetSally:Movie {title:'When Harry Met Sally', released:1998, tagline:'At odds in life... in love on-line.'})
                  |CREATE (BillyC:Person {name:'Billy Crystal', born:1948})
                  |CREATE (CarrieF:Person {name:'Carrie Fisher', born:1956})
                  |CREATE (BrunoK:Person {name:'Bruno Kirby', born:1949})
                  |CREATE
                  |  (BillyC)-[:ACTED_IN {roles:['Harry Burns']}]->(WhenHarryMetSally),
                  |  (MegR)-[:ACTED_IN {roles:['Sally Albright']}]->(WhenHarryMetSally),
                  |  (CarrieF)-[:ACTED_IN {roles:['Marie']}]->(WhenHarryMetSally),
                  |  (BrunoK)-[:ACTED_IN {roles:['Jess']}]->(WhenHarryMetSally),
                  |  (RobR)-[:DIRECTED]->(WhenHarryMetSally),
                  |  (RobR)-[:PRODUCED]->(WhenHarryMetSally),
                  |  (NoraE)-[:PRODUCED]->(WhenHarryMetSally),
                  |  (NoraE)-[:WROTE]->(WhenHarryMetSally)
                  |
                  |CREATE (ThatThingYouDo:Movie {title:'That Thing You Do', released:1996, tagline:'In every life there comes a time when that thing you dream becomes that thing you do'})
                  |CREATE (LivT:Person {name:'Liv Tyler', born:1977})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Mr. White']}]->(ThatThingYouDo),
                  |  (LivT)-[:ACTED_IN {roles:['Faye Dolan']}]->(ThatThingYouDo),
                  |  (Charlize)-[:ACTED_IN {roles:['Tina']}]->(ThatThingYouDo),
                  |  (TomH)-[:DIRECTED]->(ThatThingYouDo)
                  |
                  |CREATE (TheReplacements:Movie {title:'The Replacements', released:2000, tagline:'Pain heals, Chicks dig scars... Glory lasts forever'})
                  |CREATE (Brooke:Person {name:'Brooke Langton', born:1970})
                  |CREATE (Gene:Person {name:'Gene Hackman', born:1930})
                  |CREATE (Orlando:Person {name:'Orlando Jones', born:1968})
                  |CREATE (Howard:Person {name:'Howard Deutch', born:1950})
                  |CREATE
                  |  (Keanu)-[:ACTED_IN {roles:['Shane Falco']}]->(TheReplacements),
                  |  (Brooke)-[:ACTED_IN {roles:['Annabelle Farrell']}]->(TheReplacements),
                  |  (Gene)-[:ACTED_IN {roles:['Jimmy McGinty']}]->(TheReplacements),
                  |  (Orlando)-[:ACTED_IN {roles:['Clifford Franklin']}]->(TheReplacements),
                  |  (Howard)-[:DIRECTED]->(TheReplacements)
                  |
                  |CREATE (RescueDawn:Movie {title:'RescueDawn', released:2006, tagline:"Based on the extraordinary true story of one man's fight for freedom"})
                  |CREATE (ChristianB:Person {name:'Christian Bale', born:1974})
                  |CREATE (ZachG:Person {name:'Zach Grenier', born:1954})
                  |CREATE
                  |  (MarshallB)-[:ACTED_IN {roles:['Admiral']}]->(RescueDawn),
                  |  (ChristianB)-[:ACTED_IN {roles:['Dieter Dengler']}]->(RescueDawn),
                  |  (ZachG)-[:ACTED_IN {roles:['Squad Leader']}]->(RescueDawn),
                  |  (SteveZ)-[:ACTED_IN {roles:['Duane']}]->(RescueDawn),
                  |  (WernerH)-[:DIRECTED]->(RescueDawn)
                  |
                  |CREATE (TheBirdcage:Movie {title:'The Birdcage', released:1996, tagline:'Come as you are'})
                  |CREATE (MikeN:Person {name:'Mike Nichols', born:1931})
                  |CREATE
                  |  (Robin)-[:ACTED_IN {roles:['Armand Goldman']}]->(TheBirdcage),
                  |  (Nathan)-[:ACTED_IN {roles:['Albert Goldman']}]->(TheBirdcage),
                  |  (Gene)-[:ACTED_IN {roles:['Sen. Kevin Keeley']}]->(TheBirdcage),
                  |  (MikeN)-[:DIRECTED]->(TheBirdcage)
                  |
                  |CREATE (Unforgiven:Movie {title:'Unforgiven', released:1992, tagline:"It's a hell of a thing, killing a man"})
                  |CREATE (RichardH:Person {name:'Richard Harris', born:1930})
                  |CREATE (ClintE:Person {name:'Clint Eastwood', born:1930})
                  |CREATE
                  |  (RichardH)-[:ACTED_IN {roles:['English Bob']}]->(Unforgiven),
                  |  (ClintE)-[:ACTED_IN {roles:['Bill Munny']}]->(Unforgiven),
                  |  (Gene)-[:ACTED_IN {roles:['Little Bill Daggett']}]->(Unforgiven),
                  |  (ClintE)-[:DIRECTED]->(Unforgiven)
                  |
                  |CREATE (JohnnyMnemonic:Movie {title:'Johnny Mnemonic', released:1995, tagline:'The hottest data on earth. In the coolest head in town'})
                  |CREATE (Takeshi:Person {name:'Takeshi Kitano', born:1947})
                  |CREATE (Dina:Person {name:'Dina Meyer', born:1968})
                  |CREATE (IceT:Person {name:'Ice-T', born:1958})
                  |CREATE (RobertL:Person {name:'Robert Longo', born:1953})
                  |CREATE
                  |  (Keanu)-[:ACTED_IN {roles:['Johnny Mnemonic']}]->(JohnnyMnemonic),
                  |  (Takeshi)-[:ACTED_IN {roles:['Takahashi']}]->(JohnnyMnemonic),
                  |  (Dina)-[:ACTED_IN {roles:['Jane']}]->(JohnnyMnemonic),
                  |  (IceT)-[:ACTED_IN {roles:['J-Bone']}]->(JohnnyMnemonic),
                  |  (RobertL)-[:DIRECTED]->(JohnnyMnemonic)
                  |
                  |CREATE (CloudAtlas:Movie {title:'Cloud Atlas', released:2012, tagline:'Everything is connected'})
                  |CREATE (HalleB:Person {name:'Halle Berry', born:1966})
                  |CREATE (JimB:Person {name:'Jim Broadbent', born:1949})
                  |CREATE (TomT:Person {name:'Tom Tykwer', born:1965})
                  |CREATE (DavidMitchell:Person {name:'David Mitchell', born:1969})
                  |CREATE (StefanArndt:Person {name:'Stefan Arndt', born:1961})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Zachry', 'Dr. Henry Goose', 'Isaac Sachs', 'Dermot Hoggins']}]->(CloudAtlas),
                  |  (Hugo)-[:ACTED_IN {roles:['Bill Smoke', 'Haskell Moore', 'Tadeusz Kesselring', 'Nurse Noakes', 'Boardman Mephi', 'Old Georgie']}]->(CloudAtlas),
                  |  (HalleB)-[:ACTED_IN {roles:['Luisa Rey', 'Jocasta Ayrs', 'Ovid', 'Meronym']}]->(CloudAtlas),
                  |  (JimB)-[:ACTED_IN {roles:['Vyvyan Ayrs', 'Captain Molyneux', 'Timothy Cavendish']}]->(CloudAtlas),
                  |  (TomT)-[:DIRECTED]->(CloudAtlas),
                  |  (AndyW)-[:DIRECTED]->(CloudAtlas),
                  |  (LanaW)-[:DIRECTED]->(CloudAtlas),
                  |  (DavidMitchell)-[:WROTE]->(CloudAtlas),
                  |  (StefanArndt)-[:PRODUCED]->(CloudAtlas)
                  |
                  |CREATE (TheDaVinciCode:Movie {title:'The Da Vinci Code', released:2006, tagline:'Break The Codes'})
                  |CREATE (IanM:Person {name:'Ian McKellen', born:1939})
                  |CREATE (AudreyT:Person {name:'Audrey Tautou', born:1976})
                  |CREATE (PaulB:Person {name:'Paul Bettany', born:1971})
                  |CREATE (RonH:Person {name:'Ron Howard', born:1954})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Dr. Robert Langdon']}]->(TheDaVinciCode),
                  |  (IanM)-[:ACTED_IN {roles:['Sir Leight Teabing']}]->(TheDaVinciCode),
                  |  (AudreyT)-[:ACTED_IN {roles:['Sophie Neveu']}]->(TheDaVinciCode),
                  |  (PaulB)-[:ACTED_IN {roles:['Silas']}]->(TheDaVinciCode),
                  |  (RonH)-[:DIRECTED]->(TheDaVinciCode)
                  |
                  |CREATE (VforVendetta:Movie {title:'V for Vendetta', released:2006, tagline:'Freedom! Forever!'})
                  |CREATE (NatalieP:Person {name:'Natalie Portman', born:1981})
                  |CREATE (StephenR:Person {name:'Stephen Rea', born:1946})
                  |CREATE (JohnH:Person {name:'John Hurt', born:1940})
                  |CREATE (BenM:Person {name: 'Ben Miles', born:1967})
                  |CREATE
                  |  (Hugo)-[:ACTED_IN {roles:['V']}]->(VforVendetta),
                  |  (NatalieP)-[:ACTED_IN {roles:['Evey Hammond']}]->(VforVendetta),
                  |  (StephenR)-[:ACTED_IN {roles:['Eric Finch']}]->(VforVendetta),
                  |  (JohnH)-[:ACTED_IN {roles:['High Chancellor Adam Sutler']}]->(VforVendetta),
                  |  (BenM)-[:ACTED_IN {roles:['Dascomb']}]->(VforVendetta),
                  |  (JamesM)-[:DIRECTED]->(VforVendetta),
                  |  (AndyW)-[:PRODUCED]->(VforVendetta),
                  |  (LanaW)-[:PRODUCED]->(VforVendetta),
                  |  (JoelS)-[:PRODUCED]->(VforVendetta),
                  |  (AndyW)-[:WROTE]->(VforVendetta),
                  |  (LanaW)-[:WROTE]->(VforVendetta)
                  |
                  |CREATE (SpeedRacer:Movie {title:'Speed Racer', released:2008, tagline:'Speed has no limits'})
                  |CREATE (EmileH:Person {name:'Emile Hirsch', born:1985})
                  |CREATE (JohnG:Person {name:'John Goodman', born:1960})
                  |CREATE (SusanS:Person {name:'Susan Sarandon', born:1946})
                  |CREATE (MatthewF:Person {name:'Matthew Fox', born:1966})
                  |CREATE (ChristinaR:Person {name:'Christina Ricci', born:1980})
                  |CREATE (Rain:Person {name:'Rain', born:1982})
                  |CREATE
                  |  (EmileH)-[:ACTED_IN {roles:['Speed Racer']}]->(SpeedRacer),
                  |  (JohnG)-[:ACTED_IN {roles:['Pops']}]->(SpeedRacer),
                  |  (SusanS)-[:ACTED_IN {roles:['Mom']}]->(SpeedRacer),
                  |  (MatthewF)-[:ACTED_IN {roles:['Racer X']}]->(SpeedRacer),
                  |  (ChristinaR)-[:ACTED_IN {roles:['Trixie']}]->(SpeedRacer),
                  |  (Rain)-[:ACTED_IN {roles:['Taejo Togokahn']}]->(SpeedRacer),
                  |  (BenM)-[:ACTED_IN {roles:['Cass Jones']}]->(SpeedRacer),
                  |  (AndyW)-[:DIRECTED]->(SpeedRacer),
                  |  (LanaW)-[:DIRECTED]->(SpeedRacer),
                  |  (AndyW)-[:WROTE]->(SpeedRacer),
                  |  (LanaW)-[:WROTE]->(SpeedRacer),
                  |  (JoelS)-[:PRODUCED]->(SpeedRacer)
                  |
                  |CREATE (NinjaAssassin:Movie {title:'Ninja Assassin', released:2009, tagline:'Prepare to enter a secret world of assassins'})
                  |CREATE (NaomieH:Person {name:'Naomie Harris'})
                  |CREATE
                  |  (Rain)-[:ACTED_IN {roles:['Raizo']}]->(NinjaAssassin),
                  |  (NaomieH)-[:ACTED_IN {roles:['Mika Coretti']}]->(NinjaAssassin),
                  |  (RickY)-[:ACTED_IN {roles:['Takeshi']}]->(NinjaAssassin),
                  |  (BenM)-[:ACTED_IN {roles:['Ryan Maslow']}]->(NinjaAssassin),
                  |  (JamesM)-[:DIRECTED]->(NinjaAssassin),
                  |  (AndyW)-[:PRODUCED]->(NinjaAssassin),
                  |  (LanaW)-[:PRODUCED]->(NinjaAssassin),
                  |  (JoelS)-[:PRODUCED]->(NinjaAssassin)
                  |
                  |CREATE (TheGreenMile:Movie {title:'The Green Mile', released:1999, tagline:"Walk a mile you'll never forget."})
                  |CREATE (MichaelD:Person {name:'Michael Clarke Duncan', born:1957})
                  |CREATE (DavidM:Person {name:'David Morse', born:1953})
                  |CREATE (SamR:Person {name:'Sam Rockwell', born:1968})
                  |CREATE (GaryS:Person {name:'Gary Sinise', born:1955})
                  |CREATE (PatriciaC:Person {name:'Patricia Clarkson', born:1959})
                  |CREATE (FrankD:Person {name:'Frank Darabont', born:1959})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Paul Edgecomb']}]->(TheGreenMile),
                  |  (MichaelD)-[:ACTED_IN {roles:['John Coffey']}]->(TheGreenMile),
                  |  (DavidM)-[:ACTED_IN {roles:['Brutus "Brutal" Howell']}]->(TheGreenMile),
                  |  (BonnieH)-[:ACTED_IN {roles:['Jan Edgecomb']}]->(TheGreenMile),
                  |  (JamesC)-[:ACTED_IN {roles:['Warden Hal Moores']}]->(TheGreenMile),
                  |  (SamR)-[:ACTED_IN {roles:['"Wild Bill" Wharton']}]->(TheGreenMile),
                  |  (GaryS)-[:ACTED_IN {roles:['Burt Hammersmith']}]->(TheGreenMile),
                  |  (PatriciaC)-[:ACTED_IN {roles:['Melinda Moores']}]->(TheGreenMile),
                  |  (FrankD)-[:DIRECTED]->(TheGreenMile)
                  |
                  |CREATE (FrostNixon:Movie {title:'Frost/Nixon', released:2008, tagline:'400 million people were waiting for the truth.'})
                  |CREATE (FrankL:Person {name:'Frank Langella', born:1938})
                  |CREATE (MichaelS:Person {name:'Michael Sheen', born:1969})
                  |CREATE (OliverP:Person {name:'Oliver Platt', born:1960})
                  |CREATE
                  |  (FrankL)-[:ACTED_IN {roles:['Richard Nixon']}]->(FrostNixon),
                  |  (MichaelS)-[:ACTED_IN {roles:['David Frost']}]->(FrostNixon),
                  |  (KevinB)-[:ACTED_IN {roles:['Jack Brennan']}]->(FrostNixon),
                  |  (OliverP)-[:ACTED_IN {roles:['Bob Zelnick']}]->(FrostNixon),
                  |  (SamR)-[:ACTED_IN {roles:['James Reston, Jr.']}]->(FrostNixon),
                  |  (RonH)-[:DIRECTED]->(FrostNixon)
                  |
                  |CREATE (Hoffa:Movie {title:'Hoffa', released:1992, tagline:"He didn't want law. He wanted justice."})
                  |CREATE (DannyD:Person {name:'Danny DeVito', born:1944})
                  |CREATE (JohnR:Person {name:'John C. Reilly', born:1965})
                  |CREATE
                  |  (JackN)-[:ACTED_IN {roles:['Hoffa']}]->(Hoffa),
                  |  (DannyD)-[:ACTED_IN {roles:['Robert "Bobby" Ciaro']}]->(Hoffa),
                  |  (JTW)-[:ACTED_IN {roles:['Frank Fitzsimmons']}]->(Hoffa),
                  |  (JohnR)-[:ACTED_IN {roles:['Peter "Pete" Connelly']}]->(Hoffa),
                  |  (DannyD)-[:DIRECTED]->(Hoffa)
                  |
                  |CREATE (Apollo13:Movie {title:'Apollo 13', released:1995, tagline:'Houston, we have a problem.'})
                  |CREATE (EdH:Person {name:'Ed Harris', born:1950})
                  |CREATE (BillPax:Person {name:'Bill Paxton', born:1955})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Jim Lovell']}]->(Apollo13),
                  |  (KevinB)-[:ACTED_IN {roles:['Jack Swigert']}]->(Apollo13),
                  |  (EdH)-[:ACTED_IN {roles:['Gene Kranz']}]->(Apollo13),
                  |  (BillPax)-[:ACTED_IN {roles:['Fred Haise']}]->(Apollo13),
                  |  (GaryS)-[:ACTED_IN {roles:['Ken Mattingly']}]->(Apollo13),
                  |  (RonH)-[:DIRECTED]->(Apollo13)
                  |
                  |CREATE (Twister:Movie {title:'Twister', released:1996, tagline:"Don't Breathe. Don't Look Back."})
                  |CREATE (PhilipH:Person {name:'Philip Seymour Hoffman', born:1967})
                  |CREATE (JanB:Person {name:'Jan de Bont', born:1943})
                  |CREATE
                  |  (BillPax)-[:ACTED_IN {roles:['Bill Harding']}]->(Twister),
                  |  (HelenH)-[:ACTED_IN {roles:['Dr. Jo Harding']}]->(Twister),
                  |  (ZachG)-[:ACTED_IN {roles:['Eddie']}]->(Twister),
                  |  (PhilipH)-[:ACTED_IN {roles:['Dustin "Dusty" Davis']}]->(Twister),
                  |  (JanB)-[:DIRECTED]->(Twister)
                  |
                  |CREATE (CastAway:Movie {title:'Cast Away', released:2000, tagline:'At the edge of the world, his journey begins.'})
                  |CREATE (RobertZ:Person {name:'Robert Zemeckis', born:1951})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Chuck Noland']}]->(CastAway),
                  |  (HelenH)-[:ACTED_IN {roles:['Kelly Frears']}]->(CastAway),
                  |  (RobertZ)-[:DIRECTED]->(CastAway)
                  |
                  |CREATE (OneFlewOvertheCuckoosNest:Movie {title:"One Flew Over the Cuckoo's Nest", released:1975, tagline:"If he's crazy, what does that make you?"})
                  |CREATE (MilosF:Person {name:'Milos Forman', born:1932})
                  |CREATE
                  |  (JackN)-[:ACTED_IN {roles:['Randle McMurphy']}]->(OneFlewOvertheCuckoosNest),
                  |  (DannyD)-[:ACTED_IN {roles:['Martini']}]->(OneFlewOvertheCuckoosNest),
                  |  (MilosF)-[:DIRECTED]->(OneFlewOvertheCuckoosNest)
                  |
                  |CREATE (SomethingsGottaGive:Movie {title:"Something's Gotta Give", released:2003})
                  |CREATE (DianeK:Person {name:'Diane Keaton', born:1946})
                  |CREATE (NancyM:Person {name:'Nancy Meyers', born:1949})
                  |CREATE
                  |  (JackN)-[:ACTED_IN {roles:['Harry Sanborn']}]->(SomethingsGottaGive),
                  |  (DianeK)-[:ACTED_IN {roles:['Erica Barry']}]->(SomethingsGottaGive),
                  |  (Keanu)-[:ACTED_IN {roles:['Julian Mercer']}]->(SomethingsGottaGive),
                  |  (NancyM)-[:DIRECTED]->(SomethingsGottaGive),
                  |  (NancyM)-[:PRODUCED]->(SomethingsGottaGive),
                  |  (NancyM)-[:WROTE]->(SomethingsGottaGive)
                  |
                  |CREATE (BicentennialMan:Movie {title:'Bicentennial Man', released:1999, tagline:"One robot's 200 year journey to become an ordinary man."})
                  |CREATE (ChrisC:Person {name:'Chris Columbus', born:1958})
                  |CREATE
                  |  (Robin)-[:ACTED_IN {roles:['Andrew Marin']}]->(BicentennialMan),
                  |  (OliverP)-[:ACTED_IN {roles:['Rupert Burns']}]->(BicentennialMan),
                  |  (ChrisC)-[:DIRECTED]->(BicentennialMan)
                  |
                  |CREATE (CharlieWilsonsWar:Movie {title:"Charlie Wilson's War", released:2007, tagline:"A stiff drink. A little mascara. A lot of nerve. Who said they couldn't bring down the Soviet empire."})
                  |CREATE (JuliaR:Person {name:'Julia Roberts', born:1967})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Rep. Charlie Wilson']}]->(CharlieWilsonsWar),
                  |  (JuliaR)-[:ACTED_IN {roles:['Joanne Herring']}]->(CharlieWilsonsWar),
                  |  (PhilipH)-[:ACTED_IN {roles:['Gust Avrakotos']}]->(CharlieWilsonsWar),
                  |  (MikeN)-[:DIRECTED]->(CharlieWilsonsWar)
                  |
                  |CREATE (ThePolarExpress:Movie {title:'The Polar Express', released:2004, tagline:'This Holiday Season… Believe'})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Hero Boy', 'Father', 'Conductor', 'Hobo', 'Scrooge', 'Santa Claus']}]->(ThePolarExpress),
                  |  (RobertZ)-[:DIRECTED]->(ThePolarExpress)
                  |
                  |CREATE (ALeagueofTheirOwn:Movie {title:'A League of Their Own', released:1992, tagline:'Once in a lifetime you get a chance to do something different.'})
                  |CREATE (Madonna:Person {name:'Madonna', born:1954})
                  |CREATE (GeenaD:Person {name:'Geena Davis', born:1956})
                  |CREATE (LoriP:Person {name:'Lori Petty', born:1963})
                  |CREATE (PennyM:Person {name:'Penny Marshall', born:1943})
                  |CREATE
                  |  (TomH)-[:ACTED_IN {roles:['Jimmy Dugan']}]->(ALeagueofTheirOwn),
                  |  (GeenaD)-[:ACTED_IN {roles:['Dottie Hinson']}]->(ALeagueofTheirOwn),
                  |  (LoriP)-[:ACTED_IN {roles:['Kit Keller']}]->(ALeagueofTheirOwn),
                  |  (RosieO)-[:ACTED_IN {roles:['Doris Murphy']}]->(ALeagueofTheirOwn),
                  |  (Madonna)-[:ACTED_IN {roles:['"All the Way" Mae Mordabito']}]->(ALeagueofTheirOwn),
                  |  (BillPax)-[:ACTED_IN {roles:['Bob Hinson']}]->(ALeagueofTheirOwn),
                  |  (PennyM)-[:DIRECTED]->(ALeagueofTheirOwn)
                  |
                  |CREATE (PaulBlythe:Person {name:'Paul Blythe'})
                  |CREATE (AngelaScope:Person {name:'Angela Scope'})
                  |CREATE (JessicaThompson:Person {name:'Jessica Thompson'})
                  |CREATE (JamesThompson:Person {name:'James Thompson'})
                  |
                  |CREATE
                  |  (JamesThompson)-[:FOLLOWS]->(JessicaThompson),
                  |  (AngelaScope)-[:FOLLOWS]->(JessicaThompson),
                  |  (PaulBlythe)-[:FOLLOWS]->(AngelaScope)
                  |
                  |CREATE
                  |  (JessicaThompson)-[:REVIEWED {summary:'An amazing journey', rating:95}]->(CloudAtlas),
                  |  (JessicaThompson)-[:REVIEWED {summary:'Silly, but fun', rating:65}]->(TheReplacements),
                  |  (JamesThompson)-[:REVIEWED {summary:'The coolest football movie ever', rating:100}]->(TheReplacements),
                  |  (AngelaScope)-[:REVIEWED {summary:'Pretty funny at times', rating:62}]->(TheReplacements),
                  |  (JessicaThompson)-[:REVIEWED {summary:'Dark, but compelling', rating:85}]->(Unforgiven),
                  |  (JessicaThompson)-[:REVIEWED {summary:"Slapstick redeemed only by the Robin Williams and Gene Hackman's stellar performances", rating:45}]->(TheBirdcage),
                  |  (JessicaThompson)-[:REVIEWED {summary:'A solid romp', rating:68}]->(TheDaVinciCode),
                  |  (JamesThompson)-[:REVIEWED {summary:'Fun, but a little far fetched', rating:65}]->(TheDaVinciCode),
                  |  (JessicaThompson)-[:REVIEWED {summary:'You had me at Jerry', rating:92}]->(JerryMaguire)
                  |
                  |WITH TomH as a
                  |MATCH (a)-[:ACTED_IN]->(m)<-[:DIRECTED]-(d) RETURN a.name, m.title, d.name""".stripMargin

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 171, relationshipsCreated = 253, propertiesWritten = 564, labelsAdded = 171)
  }

  test("should work when given a lot of create clauses") {
    val query = """create (hf:School {name : 'Hilly Fields Technical College'})
                  |create (hf)-[:STAFF]->(mrb:Teacher {name : 'Mr Balls'})
                  |create (hf)-[:STAFF]->(mrspb:Teacher {name : 'Ms Packard-Bell'})
                  |create (hf)-[:STAFF]->(mrs:Teacher {name : 'Mr Smith'})
                  |create (hf)-[:STAFF]->(mrsa:Teacher {name : 'Mrs Adenough'})
                  |create (hf)-[:STAFF]->(mrvdg:Teacher {name : 'Mr Van der Graaf'})
                  |create (hf)-[:STAFF]->(msn:Teacher {name : 'Ms Noethe'})
                  |create (hf)-[:STAFF]->(mrsn:Teacher {name : 'Mrs Noakes'})
                  |create (hf)-[:STAFF]->(mrm:Teacher {name : 'Mr Marker'})
                  |create (hf)-[:STAFF]->(msd:Teacher {name : 'Ms Delgado'})
                  |create (hf)-[:STAFF]->(mrsg:Teacher {name : 'Mrs Glass'})
                  |create (hf)-[:STAFF]->(mrf:Teacher {name : 'Mr Flint'})
                  |create (hf)-[:STAFF]->(mrk:Teacher {name : 'Mr Kearney'})
                  |create (hf)-[:STAFF]->(msf:Teacher {name : 'Mrs Forrester'})
                  |create (hf)-[:STAFF]->(mrsf:Teacher {name : 'Mrs Fischer'})
                  |create (hf)-[:STAFF]->(mrj:Teacher {name : 'Mr Jameson'})
                  |
                  |create (hf)-[:STUDENT]->(_001:Student { name :'Portia Vasquez'})
                  |create (hf)-[:STUDENT]->(_002:Student { name :'Andrew Parks'})
                  |create (hf)-[:STUDENT]->(_003:Student { name :'Germane Frye'})
                  |create (hf)-[:STUDENT]->(_004:Student { name :'Yuli Gutierrez'})
                  |create (hf)-[:STUDENT]->(_005:Student { name :'Kamal Solomon'})
                  |create (hf)-[:STUDENT]->(_006:Student { name :'Lysandra Porter'})
                  |create (hf)-[:STUDENT]->(_007:Student { name :'Stella Santiago'})
                  |create (hf)-[:STUDENT]->(_008:Student { name :'Brenda Torres'})
                  |create (hf)-[:STUDENT]->(_009:Student { name :'Heidi Dunlap'})
                  |
                  |create (hf)-[:STUDENT]->(_010:Student { name :'Halee Taylor' })
                  |create (hf)-[:STUDENT]->(_011:Student { name :'Brennan Crosby' })
                  |create (hf)-[:STUDENT]->(_012:Student { name :'Rooney Cook' })
                  |create (hf)-[:STUDENT]->(_013:Student { name :'Xavier Morrison' })
                  |create (hf)-[:STUDENT]->(_014:Student { name :'Zelenia Santana' })
                  |create (hf)-[:STUDENT]->(_015:Student { name :'Eaton Bonner' })
                  |create (hf)-[:STUDENT]->(_016:Student { name :'Leilani Bishop' })
                  |create (hf)-[:STUDENT]->(_017:Student { name :'Jamalia Pickett' })
                  |create (hf)-[:STUDENT]->(_018:Student { name :'Wynter Russell' })
                  |create (hf)-[:STUDENT]->(_019:Student { name :'Liberty Melton' })
                  |
                  |create (hf)-[:STUDENT]->(_020:Student { name :'MacKensie Obrien' })
                  |create (hf)-[:STUDENT]->(_021:Student { name :'Oprah Maynard' })
                  |create (hf)-[:STUDENT]->(_022:Student { name :'Lyle Parks' })
                  |create (hf)-[:STUDENT]->(_023:Student { name :'Madonna Justice' })
                  |create (hf)-[:STUDENT]->(_024:Student { name :'Herman Frederick' })
                  |create (hf)-[:STUDENT]->(_025:Student { name :'Preston Stevenson' })
                  |create (hf)-[:STUDENT]->(_026:Student { name :'Drew Carrillo' })
                  |create (hf)-[:STUDENT]->(_027:Student { name :'Hamilton Woodward' })
                  |create (hf)-[:STUDENT]->(_028:Student { name :'Buckminster Bradley' })
                  |create (hf)-[:STUDENT]->(_029:Student { name :'Shea Cote' })
                  |
                  |create (hf)-[:STUDENT]->(_030:Student { name :'Raymond Leonard' })
                  |create (hf)-[:STUDENT]->(_031:Student { name :'Gavin Branch' })
                  |create (hf)-[:STUDENT]->(_032:Student { name :'Kylan Powers' })
                  |create (hf)-[:STUDENT]->(_033:Student { name :'Hedy Bowers' })
                  |create (hf)-[:STUDENT]->(_034:Student { name :'Derek Church' })
                  |create (hf)-[:STUDENT]->(_035:Student { name :'Silas Santiago' })
                  |create (hf)-[:STUDENT]->(_036:Student { name :'Elton Bright' })
                  |create (hf)-[:STUDENT]->(_037:Student { name :'Dora Schmidt' })
                  |create (hf)-[:STUDENT]->(_038:Student { name :'Julian Sullivan' })
                  |create (hf)-[:STUDENT]->(_039:Student { name :'Willow Morton' })
                  |
                  |create (hf)-[:STUDENT]->(_040:Student { name :'Blaze Hines' })
                  |create (hf)-[:STUDENT]->(_041:Student { name :'Felicia Tillman' })
                  |create (hf)-[:STUDENT]->(_042:Student { name :'Ralph Webb' })
                  |create (hf)-[:STUDENT]->(_043:Student { name :'Roth Gilmore' })
                  |create (hf)-[:STUDENT]->(_044:Student { name :'Dorothy Burgess' })
                  |create (hf)-[:STUDENT]->(_045:Student { name :'Lana Sandoval' })
                  |create (hf)-[:STUDENT]->(_046:Student { name :'Nevada Strickland' })
                  |create (hf)-[:STUDENT]->(_047:Student { name :'Lucian Franco' })
                  |create (hf)-[:STUDENT]->(_048:Student { name :'Jasper Talley' })
                  |create (hf)-[:STUDENT]->(_049:Student { name :'Madaline Spears' })
                  |
                  |create (hf)-[:STUDENT]->(_050:Student { name :'Upton Browning' })
                  |create (hf)-[:STUDENT]->(_051:Student { name :'Cooper Leon' })
                  |create (hf)-[:STUDENT]->(_052:Student { name :'Celeste Ortega' })
                  |create (hf)-[:STUDENT]->(_053:Student { name :'Willa Hewitt' })
                  |create (hf)-[:STUDENT]->(_054:Student { name :'Rooney Bryan' })
                  |create (hf)-[:STUDENT]->(_055:Student { name :'Nayda Hays' })
                  |create (hf)-[:STUDENT]->(_056:Student { name :'Kadeem Salazar' })
                  |create (hf)-[:STUDENT]->(_057:Student { name :'Halee Allen' })
                  |create (hf)-[:STUDENT]->(_058:Student { name :'Odysseus Mayo' })
                  |create (hf)-[:STUDENT]->(_059:Student { name :'Kato Merrill' })
                  |
                  |create (hf)-[:STUDENT]->(_060:Student { name :'Halee Juarez' })
                  |create (hf)-[:STUDENT]->(_061:Student { name :'Chloe Charles' })
                  |create (hf)-[:STUDENT]->(_062:Student { name :'Abel Montoya' })
                  |create (hf)-[:STUDENT]->(_063:Student { name :'Hilda Welch' })
                  |create (hf)-[:STUDENT]->(_064:Student { name :'Britanni Bean' })
                  |create (hf)-[:STUDENT]->(_065:Student { name :'Joelle Beach' })
                  |create (hf)-[:STUDENT]->(_066:Student { name :'Ciara Odom' })
                  |create (hf)-[:STUDENT]->(_067:Student { name :'Zia Williams' })
                  |create (hf)-[:STUDENT]->(_068:Student { name :'Darrel Bailey' })
                  |create (hf)-[:STUDENT]->(_069:Student { name :'Lance Mcdowell' })
                  |
                  |create (hf)-[:STUDENT]->(_070:Student { name :'Clayton Bullock' })
                  |create (hf)-[:STUDENT]->(_071:Student { name :'Roanna Mosley' })
                  |create (hf)-[:STUDENT]->(_072:Student { name :'Amethyst Mcclure' })
                  |create (hf)-[:STUDENT]->(_073:Student { name :'Hanae Mann' })
                  |create (hf)-[:STUDENT]->(_074:Student { name :'Graiden Haynes' })
                  |create (hf)-[:STUDENT]->(_075:Student { name :'Marcia Byrd' })
                  |create (hf)-[:STUDENT]->(_076:Student { name :'Yoshi Joyce' })
                  |create (hf)-[:STUDENT]->(_077:Student { name :'Gregory Sexton' })
                  |create (hf)-[:STUDENT]->(_078:Student { name :'Nash Carey' })
                  |create (hf)-[:STUDENT]->(_079:Student { name :'Rae Stevens' })
                  |
                  |create (hf)-[:STUDENT]->(_080:Student { name :'Blossom Fulton' })
                  |create (hf)-[:STUDENT]->(_081:Student { name :'Lev Curry' })
                  |create (hf)-[:STUDENT]->(_082:Student { name :'Margaret Gamble' })
                  |create (hf)-[:STUDENT]->(_083:Student { name :'Rylee Patterson' })
                  |create (hf)-[:STUDENT]->(_084:Student { name :'Harper Perkins' })
                  |create (hf)-[:STUDENT]->(_085:Student { name :'Kennan Murphy' })
                  |create (hf)-[:STUDENT]->(_086:Student { name :'Hilda Coffey' })
                  |create (hf)-[:STUDENT]->(_087:Student { name :'Marah Reed' })
                  |create (hf)-[:STUDENT]->(_088:Student { name :'Blaine Wade' })
                  |create (hf)-[:STUDENT]->(_089:Student { name :'Geraldine Sanders' })
                  |
                  |create (hf)-[:STUDENT]->(_090:Student { name :'Kerry Rollins' })
                  |create (hf)-[:STUDENT]->(_091:Student { name :'Virginia Sweet' })
                  |create (hf)-[:STUDENT]->(_092:Student { name :'Sophia Merrill' })
                  |create (hf)-[:STUDENT]->(_093:Student { name :'Hedda Carson' })
                  |create (hf)-[:STUDENT]->(_094:Student { name :'Tamekah Charles' })
                  |create (hf)-[:STUDENT]->(_095:Student { name :'Knox Barton' })
                  |create (hf)-[:STUDENT]->(_096:Student { name :'Ariel Porter' })
                  |create (hf)-[:STUDENT]->(_097:Student { name :'Berk Wooten' })
                  |create (hf)-[:STUDENT]->(_098:Student { name :'Galena Glenn' })
                  |create (hf)-[:STUDENT]->(_099:Student { name :'Jolene Anderson' })
                  |
                  |create (hf)-[:STUDENT]->(_100:Student { name :'Leonard Hewitt'})
                  |create (hf)-[:STUDENT]->(_101:Student { name :'Maris Salazar' })
                  |create (hf)-[:STUDENT]->(_102:Student { name :'Brian Frost' })
                  |create (hf)-[:STUDENT]->(_103:Student { name :'Zane Moses' })
                  |create (hf)-[:STUDENT]->(_104:Student { name :'Serina Finch' })
                  |create (hf)-[:STUDENT]->(_105:Student { name :'Anastasia Fletcher' })
                  |create (hf)-[:STUDENT]->(_106:Student { name :'Glenna Chapman' })
                  |create (hf)-[:STUDENT]->(_107:Student { name :'Mufutau Gillespie' })
                  |create (hf)-[:STUDENT]->(_108:Student { name :'Basil Guthrie' })
                  |create (hf)-[:STUDENT]->(_109:Student { name :'Theodore Marsh' })
                  |
                  |create (hf)-[:STUDENT]->(_110:Student { name :'Jaime Contreras' })
                  |create (hf)-[:STUDENT]->(_111:Student { name :'Irma Poole' })
                  |create (hf)-[:STUDENT]->(_112:Student { name :'Buckminster Bender' })
                  |create (hf)-[:STUDENT]->(_113:Student { name :'Elton Morris' })
                  |create (hf)-[:STUDENT]->(_114:Student { name :'Barbara Nguyen' })
                  |create (hf)-[:STUDENT]->(_115:Student { name :'Tanya Kidd' })
                  |create (hf)-[:STUDENT]->(_116:Student { name :'Kaden Hoover' })
                  |create (hf)-[:STUDENT]->(_117:Student { name :'Christopher Bean' })
                  |create (hf)-[:STUDENT]->(_118:Student { name :'Trevor Daugherty' })
                  |create (hf)-[:STUDENT]->(_119:Student { name :'Rudyard Bates' })
                  |
                  |create (hf)-[:STUDENT]->(_120:Student { name :'Stacy Monroe' })
                  |create (hf)-[:STUDENT]->(_121:Student { name :'Kieran Keller' })
                  |create (hf)-[:STUDENT]->(_122:Student { name :'Ivy Garrison' })
                  |create (hf)-[:STUDENT]->(_123:Student { name :'Miranda Haynes' })
                  |create (hf)-[:STUDENT]->(_124:Student { name :'Abigail Heath' })
                  |create (hf)-[:STUDENT]->(_125:Student { name :'Margaret Santiago' })
                  |create (hf)-[:STUDENT]->(_126:Student { name :'Cade Floyd' })
                  |create (hf)-[:STUDENT]->(_127:Student { name :'Allen Crane' })
                  |create (hf)-[:STUDENT]->(_128:Student { name :'Stella Gilliam' })
                  |create (hf)-[:STUDENT]->(_129:Student { name :'Rashad Miller' })
                  |
                  |create (hf)-[:STUDENT]->(_130:Student { name :'Francis Cox' })
                  |create (hf)-[:STUDENT]->(_131:Student { name :'Darryl Rosario' })
                  |create (hf)-[:STUDENT]->(_132:Student { name :'Michael Daniels' })
                  |create (hf)-[:STUDENT]->(_133:Student { name :'Aretha Henderson' })
                  |create (hf)-[:STUDENT]->(_134:Student { name :'Roth Barrera' })
                  |create (hf)-[:STUDENT]->(_135:Student { name :'Yael Day' })
                  |create (hf)-[:STUDENT]->(_136:Student { name :'Wynter Richmond' })
                  |create (hf)-[:STUDENT]->(_137:Student { name :'Quyn Flowers' })
                  |create (hf)-[:STUDENT]->(_138:Student { name :'Yvette Marquez' })
                  |create (hf)-[:STUDENT]->(_139:Student { name :'Teagan Curry' })
                  |
                  |create (hf)-[:STUDENT]->(_140:Student { name :'Brenden Bishop' })
                  |create (hf)-[:STUDENT]->(_141:Student { name :'Montana Black' })
                  |create (hf)-[:STUDENT]->(_142:Student { name :'Ramona Parker' })
                  |create (hf)-[:STUDENT]->(_143:Student { name :'Merritt Hansen' })
                  |create (hf)-[:STUDENT]->(_144:Student { name :'Melvin Vang' })
                  |create (hf)-[:STUDENT]->(_145:Student { name :'Samantha Perez' })
                  |create (hf)-[:STUDENT]->(_146:Student { name :'Thane Porter' })
                  |create (hf)-[:STUDENT]->(_147:Student { name :'Vaughan Haynes' })
                  |create (hf)-[:STUDENT]->(_148:Student { name :'Irma Miles' })
                  |create (hf)-[:STUDENT]->(_149:Student { name :'Amery Jensen' })
                  |
                  |create (hf)-[:STUDENT]->(_150:Student { name :'Montana Holman' })
                  |create (hf)-[:STUDENT]->(_151:Student { name :'Kimberly Langley' })
                  |create (hf)-[:STUDENT]->(_152:Student { name :'Ebony Bray' })
                  |create (hf)-[:STUDENT]->(_153:Student { name :'Ishmael Pollard' })
                  |create (hf)-[:STUDENT]->(_154:Student { name :'Illana Thompson' })
                  |create (hf)-[:STUDENT]->(_155:Student { name :'Rhona Bowers' })
                  |create (hf)-[:STUDENT]->(_156:Student { name :'Lilah Dotson' })
                  |create (hf)-[:STUDENT]->(_157:Student { name :'Shelly Roach' })
                  |create (hf)-[:STUDENT]->(_158:Student { name :'Celeste Woodward' })
                  |create (hf)-[:STUDENT]->(_159:Student { name :'Christen Lynn' })
                  |
                  |create (hf)-[:STUDENT]->(_160:Student { name :'Miranda Slater' })
                  |create (hf)-[:STUDENT]->(_161:Student { name :'Lunea Clements' })
                  |create (hf)-[:STUDENT]->(_162:Student { name :'Lester Francis' })
                  |create (hf)-[:STUDENT]->(_163:Student { name :'David Fischer' })
                  |create (hf)-[:STUDENT]->(_164:Student { name :'Kyra Bean' })
                  |create (hf)-[:STUDENT]->(_165:Student { name :'Imelda Alston' })
                  |create (hf)-[:STUDENT]->(_166:Student { name :'Finn Farrell' })
                  |create (hf)-[:STUDENT]->(_167:Student { name :'Kirby House' })
                  |create (hf)-[:STUDENT]->(_168:Student { name :'Amanda Zamora' })
                  |create (hf)-[:STUDENT]->(_169:Student { name :'Rina Franco' })
                  |
                  |create (hf)-[:STUDENT]->(_170:Student { name :'Sonia Lane' })
                  |create (hf)-[:STUDENT]->(_171:Student { name :'Nora Jefferson' })
                  |create (hf)-[:STUDENT]->(_172:Student { name :'Colton Ortiz' })
                  |create (hf)-[:STUDENT]->(_173:Student { name :'Alden Munoz' })
                  |create (hf)-[:STUDENT]->(_174:Student { name :'Ferdinand Cline' })
                  |create (hf)-[:STUDENT]->(_175:Student { name :'Cynthia Prince' })
                  |create (hf)-[:STUDENT]->(_176:Student { name :'Asher Hurst' })
                  |create (hf)-[:STUDENT]->(_177:Student { name :'MacKensie Stevenson' })
                  |create (hf)-[:STUDENT]->(_178:Student { name :'Sydnee Sosa' })
                  |create (hf)-[:STUDENT]->(_179:Student { name :'Dante Callahan' })
                  |
                  |create (hf)-[:STUDENT]->(_180:Student { name :'Isabella Santana' })
                  |create (hf)-[:STUDENT]->(_181:Student { name :'Raven Bowman' })
                  |create (hf)-[:STUDENT]->(_182:Student { name :'Kirby Bolton' })
                  |create (hf)-[:STUDENT]->(_183:Student { name :'Peter Shaffer' })
                  |create (hf)-[:STUDENT]->(_184:Student { name :'Fletcher Beard' })
                  |create (hf)-[:STUDENT]->(_185:Student { name :'Irene Lowe' })
                  |create (hf)-[:STUDENT]->(_186:Student { name :'Ella Talley' })
                  |create (hf)-[:STUDENT]->(_187:Student { name :'Jorden Kerr' })
                  |create (hf)-[:STUDENT]->(_188:Student { name :'Macey Delgado' })
                  |create (hf)-[:STUDENT]->(_189:Student { name :'Ulysses Graves' })
                  |
                  |create (hf)-[:STUDENT]->(_190:Student { name :'Declan Blake' })
                  |create (hf)-[:STUDENT]->(_191:Student { name :'Lila Hurst' })
                  |create (hf)-[:STUDENT]->(_192:Student { name :'David Rasmussen' })
                  |create (hf)-[:STUDENT]->(_193:Student { name :'Desiree Cortez' })
                  |create (hf)-[:STUDENT]->(_194:Student { name :'Myles Horton' })
                  |create (hf)-[:STUDENT]->(_195:Student { name :'Rylee Willis' })
                  |create (hf)-[:STUDENT]->(_196:Student { name :'Kelsey Yates' })
                  |create (hf)-[:STUDENT]->(_197:Student { name :'Alika Stanton' })
                  |create (hf)-[:STUDENT]->(_198:Student { name :'Ria Campos' })
                  |create (hf)-[:STUDENT]->(_199:Student { name :'Elijah Hendricks'})
                  |
                  |create (hf)-[:STUDENT]->(_200:Student { name :'Hayes House' })
                  |
                  |create (hf)-[:DEPARTMENT]->(md:Department {name : 'Mathematics' })
                  |create (hf)-[:DEPARTMENT]->(sd:Department {name : 'Science' })
                  |create (hf)-[:DEPARTMENT]->(ed:Department {name : 'Engineering'})
                  |create (pm:Subject {name : 'Pure Mathematics'})
                  |create (am:Subject {name : 'Applied Mathematics'})
                  |create (ph:Subject {name : 'Physics'})
                  |create (ch:Subject {name : 'Chemistry'})
                  |create (bi:Subject {name : 'Biology'})
                  |create (es:Subject {name : 'Earth Science'})
                  |create (me:Subject {name : 'Mechanical Engineering'})
                  |create (ce:Subject {name : 'Chemical Engineering'})
                  |create (se:Subject {name : 'Systems Engineering'})
                  |create (ve:Subject {name : 'Civil Engineering'})
                  |create (ee:Subject {name : 'Electrical Engineering'})
                  |create (sd)-[:CURRICULUM]->(ph)
                  |create (sd)-[:CURRICULUM]->(ch)
                  |create (sd)-[:CURRICULUM]->(bi)
                  |create (sd)-[:CURRICULUM]->(es)
                  |create (md)-[:CURRICULUM]->(pm)
                  |create (md)-[:CURRICULUM]->(am)
                  |create (ed)-[:CURRICULUM]->(me)
                  |create (ed)-[:CURRICULUM]->(se)
                  |create (ed)-[:CURRICULUM]->(ce)
                  |create (ed)-[:CURRICULUM]->(ee)
                  |create (ed)-[:CURRICULUM]->(ve)
                  |create (ph)-[:TAUGHT_BY]->(mrb)
                  |create (ph)-[:TAUGHT_BY]->(mrk)
                  |create (ch)-[:TAUGHT_BY]->(mrk)
                  |create (ch)-[:TAUGHT_BY]->(mrsn)
                  |create (bi)-[:TAUGHT_BY]->(mrsn)
                  |create (bi)-[:TAUGHT_BY]->(mrsf)
                  |create (es)-[:TAUGHT_BY]->(msn)
                  |create (pm)-[:TAUGHT_BY]->(mrf)
                  |create (pm)-[:TAUGHT_BY]->(mrm)
                  |create (pm)-[:TAUGHT_BY]->(mrvdg)
                  |create (am)-[:TAUGHT_BY]->(mrsg)
                  |create (am)-[:TAUGHT_BY]->(mrspb)
                  |create (am)-[:TAUGHT_BY]->(mrvdg)
                  |create (me)-[:TAUGHT_BY]->(mrj)
                  |create (ce)-[:TAUGHT_BY]->(mrsa)
                  |create (se)-[:TAUGHT_BY]->(mrs)
                  |create (ve)-[:TAUGHT_BY]->(msd)
                  |create (ee)-[:TAUGHT_BY]->(mrsf)
                  |
                  |create(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
                  |create(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
                  |create(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_106)
                  |create(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
                  |create(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_153)
                  |create(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
                  |create(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
                  |create(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
                  |create(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
                  |create(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
                  |create(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_043)
                  |create(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
                  |create(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
                  |create(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_165)
                  |create(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
                  |create(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
                  |create(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_062)
                  |create(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_033)
                  |create(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
                  |create(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
                  |create(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
                  |create(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_121)
                  |create(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_049)
                  |create(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
                  |create(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
                  |create(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_085)
                  |create(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
                  |create(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
                  |create(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
                  |create(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
                  |create(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
                  |create(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
                  |create(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
                  |create(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
                  |create(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
                  |create(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
                  |create(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_049)
                  |create(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
                  |create(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
                  |create(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_149)
                  |create(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
                  |create(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
                  |create(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
                  |create(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
                  |create(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
                  |create(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_189)
                  |create(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
                  |create(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
                  |create(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
                  |create(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
                  |create(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_148)
                  |create(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
                  |create(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
                  |create(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
                  |create(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_042)
                  |create(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
                  |create(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
                  |create(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_034)
                  |create(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_088)
                  |create(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
                  |create(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_88)
                  |create(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
                  |create(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
                  |create(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
                  |create(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
                  |create(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
                  |create(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
                  |create(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
                  |create(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
                  |create(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
                  |create(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_128)
                  |create(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_149)
                  |create(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
                  |create(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_104)
                  |create(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
                  |create(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
                  |create(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
                  |create(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
                  |create(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
                  |create(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
                  |create(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
                  |create(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_067)
                  |create(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
                  |create(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_128)
                  |create(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
                  |create(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
                  |create(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_137)
                  |create(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
                  |create(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_156)
                  |create(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
                  |create(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
                  |create(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
                  |create(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_006)
                  |create(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
                  |create(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
                  |create(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_001)
                  |create(_101)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
                  |create(_102)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
                  |create(_103)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_136)
                  |create(_104)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
                  |create(_105)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_127)
                  |create(_106)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
                  |create(_107)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_036)
                  |create(_108)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
                  |create(_109)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
                  |create(_110)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
                  |create(_111)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
                  |create(_112)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
                  |create(_113)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_035)
                  |create(_114)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
                  |create(_115)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_070)
                  |create(_116)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
                  |create(_117)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
                  |create(_118)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
                  |create(_119)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
                  |create(_120)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_167)
                  |create(_121)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
                  |create(_122)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
                  |create(_123)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_004)
                  |create(_124)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
                  |create(_125)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
                  |create(_126)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
                  |create(_127)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_168)
                  |create(_128)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
                  |create(_129)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
                  |create(_130)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
                  |create(_131)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
                  |create(_132)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
                  |create(_133)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
                  |create(_134)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_037)
                  |create(_135)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_012)
                  |create(_136)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
                  |create(_137)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
                  |create(_138)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
                  |create(_139)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
                  |create(_140)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_170)
                  |create(_141)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
                  |create(_142)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_139)
                  |create(_143)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
                  |create(_144)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
                  |create(_145)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
                  |create(_146)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
                  |create(_147)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
                  |create(_148)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
                  |create(_149)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_155)
                  |create(_150)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_151)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
                  |create(_152)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
                  |create(_153)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_179)
                  |create(_154)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
                  |create(_155)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_058)
                  |create(_156)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_071)
                  |create(_157)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
                  |create(_158)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
                  |create(_159)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
                  |create(_160)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
                  |create(_161)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
                  |create(_162)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
                  |create(_163)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_163)
                  |create(_164)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
                  |create(_165)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
                  |create(_166)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_136)
                  |create(_167)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_038)
                  |create(_168)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_110)
                  |create(_169)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
                  |create(_170)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
                  |create(_171)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
                  |create(_172)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
                  |create(_173)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
                  |create(_174)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
                  |create(_175)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
                  |create(_176)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
                  |create(_177)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
                  |create(_178)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_006)
                  |create(_179)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
                  |create(_180)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_185)
                  |create(_181)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
                  |create(_182)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
                  |create(_183)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
                  |create(_184)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
                  |create(_185)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
                  |create(_186)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
                  |create(_187)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
                  |create(_188)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
                  |create(_189)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
                  |create(_190)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_063)
                  |create(_191)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_079)
                  |create(_192)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_121)
                  |create(_193)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
                  |create(_194)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
                  |create(_195)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
                  |create(_196)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
                  |create(_197)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
                  |create(_198)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_039)
                  |create(_199)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
                  |create(_200)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
                  |create(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
                  |create(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_078)
                  |create(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
                  |create(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
                  |create(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
                  |create(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
                  |create(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
                  |create(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
                  |create(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_147)
                  |create(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
                  |create(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_118)
                  |create(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
                  |create(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
                  |create(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
                  |create(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
                  |create(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
                  |create(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
                  |create(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
                  |create(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
                  |create(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
                  |create(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
                  |create(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_075)
                  |create(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_087)
                  |create(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_163)
                  |create(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
                  |create(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_042)
                  |create(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_058)
                  |create(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_188)
                  |create(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
                  |create(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
                  |create(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
                  |create(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_158)
                  |create(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
                  |create(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_102)
                  |create(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
                  |create(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
                  |create(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
                  |create(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
                  |create(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
                  |create(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_085)
                  |create(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_025)
                  |create(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
                  |create(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_118)
                  |create(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
                  |create(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_071)
                  |create(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
                  |create(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
                  |create(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_059)
                  |create(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_095)
                  |create(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_185)
                  |create(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
                  |create(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
                  |create(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
                  |create(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
                  |create(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
                  |create(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
                  |create(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
                  |create(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
                  |create(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
                  |create(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
                  |create(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
                  |create(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_153)
                  |create(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
                  |create(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
                  |create(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
                  |create(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
                  |create(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
                  |create(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
                  |create(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
                  |create(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_174)
                  |create(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
                  |create(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_179)
                  |create(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_005)
                  |create(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
                  |create(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_017)
                  |create(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
                  |create(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
                  |create(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
                  |create(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_099)
                  |create(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_161)
                  |create(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
                  |create(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
                  |create(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
                  |create(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_164)
                  |create(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_064)
                  |create(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
                  |create(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_077)
                  |create(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_124)
                  |create(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
                  |create(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_142)
                  |create(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
                  |create(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_093)
                  |create(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
                  |create(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
                  |create(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_182)
                  |create(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_043)
                  |create(_101)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
                  |create(_102)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_141)
                  |create(_103)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_040)
                  |create(_104)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
                  |create(_105)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_063)
                  |create(_106)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_180)
                  |create(_107)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_010)
                  |create(_108)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
                  |create(_109)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
                  |create(_110)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
                  |create(_111)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_199)
                  |create(_112)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
                  |create(_113)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
                  |create(_114)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_096)
                  |create(_115)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_028)
                  |create(_116)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
                  |create(_117)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_191)
                  |create(_118)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_169)
                  |create(_119)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
                  |create(_120)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
                  |create(_121)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_032)
                  |create(_122)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_127)
                  |create(_123)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
                  |create(_124)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
                  |create(_125)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
                  |create(_126)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_175)
                  |create(_127)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
                  |create(_128)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_165)
                  |create(_129)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
                  |create(_130)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_066)
                  |create(_131)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
                  |create(_132)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
                  |create(_133)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_111)
                  |create(_134)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
                  |create(_135)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
                  |create(_136)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
                  |create(_137)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_181)
                  |create(_138)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
                  |create(_139)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_115)
                  |create(_140)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_013)
                  |create(_141)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
                  |create(_142)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_003)
                  |create(_143)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
                  |create(_144)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_145)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
                  |create(_146)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_061)
                  |create(_147)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_009)
                  |create(_148)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
                  |create(_149)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
                  |create(_150)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_152)
                  |create(_151)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_055)
                  |create(_152)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_157)
                  |create(_153)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
                  |create(_154)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_162)
                  |create(_155)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_146)
                  |create(_156)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_073)
                  |create(_157)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_044)
                  |create(_158)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_154)
                  |create(_159)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_160)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_168)
                  |create(_161)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
                  |create(_162)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_015)
                  |create(_163)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
                  |create(_164)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_087)
                  |create(_165)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_104)
                  |create(_166)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
                  |create(_167)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_019)
                  |create(_168)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_021)
                  |create(_169)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_065)
                  |create(_170)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_183)
                  |create(_171)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_147)
                  |create(_172)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
                  |create(_173)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
                  |create(_174)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_137)
                  |create(_175)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
                  |create(_176)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
                  |create(_177)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_078)
                  |create(_178)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_176)
                  |create(_179)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_062)
                  |create(_180)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
                  |create(_181)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_178)
                  |create(_182)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_173)
                  |create(_183)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
                  |create(_184)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_198)
                  |create(_185)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
                  |create(_186)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_041)
                  |create(_187)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
                  |create(_188)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
                  |create(_189)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_093)
                  |create(_190)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_191)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_183)
                  |create(_192)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_140)
                  |create(_193)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
                  |create(_194)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_117)
                  |create(_195)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_054)
                  |create(_196)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_197)
                  |create(_197)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_086)
                  |create(_198)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_190)
                  |create(_199)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
                  |create(_200)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_144)
                  |create(_001)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_050)
                  |create(_002)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_024)
                  |create(_003)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
                  |create(_004)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_094)
                  |create(_005)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_143)
                  |create(_006)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_066)
                  |create(_007)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_193)
                  |create(_008)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_022)
                  |create(_009)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_074)
                  |create(_010)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
                  |create(_011)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
                  |create(_012)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_036)
                  |create(_013)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
                  |create(_014)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_108)
                  |create(_015)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_083)
                  |create(_016)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
                  |create(_017)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_016)
                  |create(_018)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)
                  |create(_019)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_013)
                  |create(_020)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_186)
                  |create(_021)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_026)
                  |create(_022)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_040)
                  |create(_023)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_064)
                  |create(_024)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_072)
                  |create(_025)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_017)
                  |create(_026)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_159)
                  |create(_027)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_076)
                  |create(_028)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_014)
                  |create(_029)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_089)
                  |create(_030)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_157)
                  |create(_031)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_029)
                  |create(_032)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_184)
                  |create(_033)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
                  |create(_034)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
                  |create(_035)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_051)
                  |create(_036)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_031)
                  |create(_037)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
                  |create(_038)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_057)
                  |create(_039)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_023)
                  |create(_040)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_109)
                  |create(_041)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_177)
                  |create(_042)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_020)
                  |create(_043)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
                  |create(_044)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
                  |create(_045)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
                  |create(_046)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
                  |create(_047)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_154)
                  |create(_048)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_090)
                  |create(_049)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_166)
                  |create(_050)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_150)
                  |create(_051)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_045)
                  |create(_052)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_123)
                  |create(_053)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_160)
                  |create(_054)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_088)
                  |create(_055)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_196)
                  |create(_056)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
                  |create(_057)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_110)
                  |create(_058)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_060)
                  |create(_059)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_084)
                  |create(_060)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_030)
                  |create(_061)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_170)
                  |create(_062)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_027)
                  |create(_063)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
                  |create(_064)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_004)
                  |create(_065)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
                  |create(_066)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_009)
                  |create(_067)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
                  |create(_068)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_077)
                  |create(_069)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_112)
                  |create(_070)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_069)
                  |create(_071)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_018)
                  |create(_072)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_172)
                  |create(_073)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_053)
                  |create(_074)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_098)
                  |create(_075)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_068)
                  |create(_076)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_132)
                  |create(_077)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_134)
                  |create(_078)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_138)
                  |create(_079)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_080)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_125)
                  |create(_081)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_129)
                  |create(_082)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_048)
                  |create(_083)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_145)
                  |create(_084)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_101)
                  |create(_085)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_131)
                  |create(_086)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_011)
                  |create(_087)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_200)
                  |create(_088)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_070)
                  |create(_089)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_008)
                  |create(_090)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_107)
                  |create(_091)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_002)
                  |create(_092)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_180)
                  |create(_093)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_001)
                  |create(_094)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_120)
                  |create(_095)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_135)
                  |create(_096)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_116)
                  |create(_097)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_171)
                  |create(_098)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_122)
                  |create(_099)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_100)
                  |create(_100)-[:BUDDY]->(:StudyBuddy)<-[:BUDDY]-(_130)""".stripMargin

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 731, relationshipsCreated = 1247, propertiesWritten = 230, labelsAdded = 730)
  }

  test("should not create nodes when aliases are applied to variable names") {
    createNode()

    val query = "MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[:T]->(b) RETURN id(a) as a, id(b) as b"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, relationshipsCreated = 1)
    result.toList should equal(List(Map("a" -> 0, "b" -> 0)))
  }

  test("should create only one node when an alias is applied to a variable name") {
    createNode()

    val query = "MATCH (n) WITH n AS a CREATE (a)-[:T]->() RETURN id(a) as a"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)
    result.toList should equal(List(Map("a" -> 0)))
  }

  test("should not create nodes when aliases are applied to variable names multiple times") {
    createNode()

    val query = "MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[:T]->(b) WITH a AS x, b AS y CREATE (x)-[:T]->(y) RETURN id(x) as x, id(y) as y"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, relationshipsCreated = 2)
    result.toList should equal(List(Map("x" -> 0, "y" -> 0)))
  }

  test("should create only one node when an alias is applied to a variable name multiple times") {
    createNode()

    val query = "MATCH (n) WITH n AS a CREATE (a)-[:T]->() WITH a AS x CREATE (x)-[:T]->() RETURN id(x) as x"

    val result = updateWithBothPlannersAndCompatibilityMode(query)
    assertStats(result, nodesCreated = 2, relationshipsCreated = 2)
    result.toList should equal(List(Map("x" -> 0)))
  }

  test("should have bound node recognized after projection with WITH + WITH") {
    val query = "CREATE (a) WITH a WITH * CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should have bound node recognized after projection with WITH + UNWIND") {
    val query = "CREATE (a) WITH a UNWIND [0] AS i CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should have bound node recognized after projection with WITH + LOAD CSV") {
    val url = createCSVTempFileURL( writer => writer.println("Foo") )

    val query = s"CREATE (a) WITH a LOAD CSV FROM '$url' AS line CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should have bound node recognized after projection with WITH + CALL") {
    val query = "CREATE (a:L) WITH a CALL db.labels() YIELD label CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWithCostPlannerOnly(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, labelsAdded = 1)
  }

  test("should have bound node recognized after projection with WITH + FOREACH") {
    val query = "CREATE (a) WITH a FOREACH (i in [] | SET a.prop = 1) CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should have bound node recognized after projection with WITH + MERGE node") {
    val query = "CREATE (a) WITH a MERGE (c) CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("should have bound node recognized after projection with WITH + MERGE pattern") {
    val query = "CREATE (a) WITH a MERGE (c)-[:T]->() CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 4, relationshipsCreated = 2)
  }
}
