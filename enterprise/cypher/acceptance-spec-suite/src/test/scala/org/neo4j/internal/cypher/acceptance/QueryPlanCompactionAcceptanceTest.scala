/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.opencypher.v9_0.util.test_helpers.WindowsStringSafe
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class QueryPlanCompactionAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport
  with CypherComparisonSupport {

  val expectedToSucceed = Configs.Interpreted - Configs.Cost2_3

  implicit val windowsSafe = WindowsStringSafe

  test("Compact very long query containing consecutive update operations") {
    val query =
      """CREATE (TheMatrix:Movie {title:'The Matrix', released:2001, tagline:'Welcome to the Real World3'})
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
        |CREATE (Emil:Person {name:'Emil Eifrem', born:1978})
        |CREATE (Emil)-[:ACTED_IN {roles:['Emil']}]->(TheMatrix)
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
        |CREATE (AFewGoodMen:Movie {title:'A Few Good Men', released:1992, tagline:"In the heart of the nation's capital, in a courthouse of the U.S. government, one man will stop at nothing to keep his honor, and one will stop at nothing to find the truth."})
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
        |CREATE (TopGun:Movie {title:'Top Gun', released:1986, tagline:'I feel the need, the need for speed.'})
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
        |CREATE (JonathanL:Person {name:'Jonathan Lipnicki', born:1990})
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
        |CREATE (StandByMe:Movie {title:'Stand By Me', released:1995, tagline:"For some, it's the last real taste of innocence, and the first real taste of life. But for everyone, it's the time that memories are made of."})
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
        |  (AnnabellaS)-[:ACTED_IN {roles:['Simon Bishop']}]->(WhatDreamsMayCome),
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
        |  (SamR)-[:ACTED_IN {roles:["'Wild Bill' Wharton"]}]->(TheGreenMile),
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
        |  (DannyD)-[:ACTED_IN {roles:["Robert 'Bobby' Ciaro"]}]->(Hoffa),
        |  (JTW)-[:ACTED_IN {roles:['Frank Fitzsimmons']}]->(Hoffa),
        |  (JohnR)-[:ACTED_IN {roles:["Peter 'Pete' Connelly"]}]->(Hoffa),
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
        |  (PhilipH)-[:ACTED_IN {roles:["Dustin 'Dusty' Davis"]}]->(Twister),
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
        |CREATE (SomethingsGottaGive:Movie {title:"Something's Gotta Give", released:1975})
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
        |CREATE (ThePolarExpress:Movie {title:'The Polar Express', released:2004, tagline:'This Holiday Season... Believe'})
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
        |  (Madonna)-[:ACTED_IN {roles:["'All the Way' Mae Mordabito"]}]->(ALeagueofTheirOwn),
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
        |;""".stripMargin


      // Removed produceResults part of plan which differs between scenarios and isn't what we want to test
      val expectedPlan =
      """
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(12) |              1 | anon[26936], anon[26983], anon[27029], anon[27083], anon[27172], anon[27260], anon[27364], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(4)          |              1 | AngelaScope, JamesThompson, JessicaThompson, PaulBlythe -- anon[10142], anon[10200], ...             |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7)  |              1 | anon[26234], anon[26303], anon[26373], anon[26441], anon[26512], anon[26598], anon[26666], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | ALeagueofTheirOwn, GeenaD, LoriP, Madonna, PennyM -- anon[10142], anon[10200], anon[10266], ...      |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(2)  |              1 | anon[25692], anon[25814] -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode             |              1 | ThePolarExpress -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], anon[10464], ... |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(4)  |              1 | anon[25305], anon[25382], anon[25455], anon[25526] -- anon[10142], anon[10200], anon[10266], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | CharlieWilsonsWar, JuliaR -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...    |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(3)  |              1 | anon[24871], anon[24940], anon[25008] -- anon[10142], anon[10200], anon[10266], anon[10333], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | BicentennialMan, ChrisC -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...      |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(6)  |              1 | anon[24313], anon[24386], anon[24456], anon[24529], anon[24576], anon[24623] -- anon[10142], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | DianeK, NancyM, SomethingsGottaGive -- anon[10142], anon[10200], anon[10266], anon[10333], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(3)  |              1 | anon[23905], anon[23986], anon[24059] -- anon[10142], anon[10200], anon[10266], anon[10333], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | MilosF, OneFlewOvertheCuckoosNest -- anon[10142], anon[10200], anon[10266], anon[10333], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(3)  |              1 | anon[23535], anon[23596], anon[23658] -- anon[10142], anon[10200], anon[10266], anon[10333], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | CastAway, RobertZ -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...            |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[23078], anon[23138], anon[23199], anon[23253], anon[23319] -- anon[10142], anon[10200], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | JanB, PhilipH, Twister -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(6)  |              1 | anon[22520], anon[22579], anon[22637], anon[22697], anon[22755], anon[22815] -- anon[10142], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | Apollo13, BillPax, EdH -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[22035], anon[22086], anon[22149], anon[22211], anon[22278] -- anon[10142], anon[10200], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | DannyD, Hoffa, JohnR -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(6)  |              1 | anon[21458], anon[21524], anon[21586], anon[21650], anon[21710], anon[21776] -- anon[10142], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(4)          |              1 | FrankL, FrostNixon, MichaelS, OliverP -- anon[10142], anon[10200], anon[10266], anon[10333], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(9)  |              1 | anon[20565], anon[20633], anon[20697], anon[20773], anon[20838], anon[20906], anon[20977], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(7)          |              1 | DavidM, FrankD, GaryS, MichaelD, PatriciaC, SamR, TheGreenMile -- anon[10142], anon[10200], ...      |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(8)  |              1 | anon[19686], anon[19746], anon[19811], anon[19870], anon[19935], anon[19975], anon[20015], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | NaomieH, NinjaAssassin -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(12) |              1 | anon[18917], anon[18978], anon[19033], anon[19089], anon[19151], anon[19206], anon[19269], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(7)          |              1 | ChristinaR, EmileH, JohnG, MatthewF, Rain, SpeedRacer, SusanS -- anon[10142], anon[10200], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(11) |              1 | anon[17929], anon[17985], anon[18052], anon[18114], anon[18192], anon[18252], anon[18291], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | BenM, JohnH, NatalieP, StephenR, VforVendetta -- anon[10142], anon[10200], anon[10266], ...          |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[17294], anon[17365], anon[17439], anon[17505], anon[17563] -- anon[10142], anon[10200], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | AudreyT, IanM, PaulB, RonH, TheDaVinciCode -- anon[10142], anon[10200], anon[10266], ...             |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(9)  |              1 | anon[16323], anon[16430], anon[16579], anon[16672], anon[16773], anon[16810], anon[16847], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(6)          |              1 | CloudAtlas, DavidMitchell, HalleB, JimB, StefanArndt, TomT -- anon[10142], anon[10200], ...          |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[15638], anon[15709], anon[15771], anon[15828], anon[15890] -- anon[10142], anon[10200], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | Dina, IceT, JohnnyMnemonic, RobertL, Takeshi -- anon[10142], anon[10200], anon[10266], ...           |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(4)  |              1 | anon[15051], anon[15113], anon[15172], anon[15242] -- anon[10142], anon[10200], anon[10266], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | ClintE, RichardH, Unforgiven -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ... |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(4)  |              1 | anon[14576], anon[14642], anon[14706], anon[14774] -- anon[10142], anon[10200], anon[10266], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | MikeN, TheBirdcage -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...           |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[14139], anon[14201], anon[14265], anon[14328], anon[14385] -- anon[10142], anon[10200], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | ChristianB, RescueDawn, ZachG -- anon[10142], anon[10200], anon[10266], anon[10333], ...             |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[13546], anon[13613], anon[13684], anon[13754], anon[13827] -- anon[10142], anon[10200], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | Brooke, Gene, Howard, Orlando, TheReplacements -- anon[10142], anon[10200], anon[10266], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(4)  |              1 | anon[12949], anon[13011], anon[13078], anon[13135] -- anon[10142], anon[10200], anon[10266], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(2)          |              1 | LivT, ThatThingYouDo -- anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(8)  |              1 | anon[12288], anon[12355], anon[12428], anon[12491], anon[12551], anon[12594], anon[12638], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(4)          |              1 | BillyC, BrunoK, CarrieF, WhenHarryMetSally -- anon[10142], anon[10200], anon[10266], ...             |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(4)  |              1 | anon[11699], anon[11766], anon[11876], anon[11938] -- anon[10142], anon[10200], anon[10266], ...     |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(3)          |              1 | JoeVersustheVolcano, JohnS, Nathan -- anon[10142], anon[10200], anon[10266], anon[10333], ...        |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7)  |              1 | anon[11001], anon[11069], anon[11137], anon[11202], anon[11268], anon[11331], anon[11394], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | BillPull, RitaW, RosieO, SleeplessInSeattle, VictorG -- anon[10142], anon[10200], anon[10266], ...   |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7)  |              1 | anon[10142], anon[10200], anon[10266], anon[10333], anon[10398], anon[10464], anon[10529], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(6)          |              1 | DaveC, NoraE, ParkerP, SteveZ, TomH, YouveGotMail -- anon[1074], anon[1135], anon[1202], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[9404], anon[9479], anon[9551], anon[9627], anon[9701] -- anon[1074], anon[1135], ...            |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(5)          |              1 | EthanH, JamesC, RickY, ScottH, SnowFallingonCedars -- anon[1074], anon[1135], anon[1202], ...        |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(6)  |              1 | anon[8661], anon[8731], anon[8805], anon[8873], anon[8943], anon[9011] -- anon[1074], ...            |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(6)          |              1 | AnnabellaS, MaxS, Robin, VincentW, WernerH, WhatDreamsMayCome -- anon[1074], anon[1135], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(5)  |              1 | anon[7906], anon[7973], anon[8041], anon[8107], anon[8173] -- anon[1074], anon[1135], ...            |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(4)          |              1 | AsGoodAsItGets, GregK, HelenH, JamesB -- anon[1074], anon[1135], anon[1202], anon[1266], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(8)  |              1 | anon[7119], anon[7184], anon[7248], anon[7309], anon[7371], anon[7436], anon[7500], anon[7560], ...  |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(6)          |              1 | CoreyF, JohnC, MarshallB, RiverP, StandByMe, WilW -- anon[1074], anon[1135], anon[1202], ...         |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(12) |              1 | anon[5913], anon[5978], anon[6042], anon[6107], anon[6172], anon[6236], anon[6299], anon[6364], ...  |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(9)          |              1 | BonnieH, CameronC, JayM, JerryMaguire, JerryO, JonathanL, KellyP, ReginaK, ReneeZ -- anon[1074], ... |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(8)  |              1 | anon[4960], anon[5015], anon[5067], anon[5122], anon[5172], anon[5222], anon[5274], anon[5306], ...  |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(8)          |              1 | AnthonyE, JimC, KellyM, MegR, TomS, TonyS, TopGun, ValK -- anon[1074], anon[1135], anon[1202], ...   |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(14) |              1 | anon[3556], anon[3624], anon[3696], anon[3772], anon[3840], anon[3912], anon[3982], anon[4052], ...  |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(14)         |              1 | AFewGoodMen, AaronS, ChristopherG, CubaG, DemiM, JTW, JackN, JamesM, KevinB, KevinP, KieferS, ...    |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(4)  |              1 | anon[2316], anon[2387], anon[2455], anon[2524] -- anon[1074], anon[1135], anon[1202], ...            |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(4)          |              1 | Al, Charlize, Taylor, TheDevilsAdvocate -- anon[1074], anon[1135], anon[1202], anon[1266], ...       |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7)  |              1 | anon[1611], anon[1675], anon[1745], anon[1812], anon[1883], anon[1930], anon[1977], ...              |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode             |              1 | TheMatrixRevolutions -- anon[1074], anon[1135], anon[1202], anon[1266], anon[1334], anon[1378], ...  |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7)  |              1 | anon[1074], anon[1135], anon[1202], anon[1266], anon[1334], anon[1378], anon[1422] -- anon[517], ... |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode             |              1 | TheMatrixReloaded -- anon[517], anon[570], anon[629], anon[685], anon[745], anon[781], ...           |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship     |              1 | anon[910] -- anon[517], anon[570], anon[629], anon[685], anon[745], anon[781], anon[817], AndyW, ... |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode             |              1 | Emil -- anon[517], anon[570], anon[629], anon[685], anon[745], anon[781], anon[817], AndyW, ...      |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7)  |              1 | anon[517], anon[570], anon[629], anon[685], anon[745], anon[781], anon[817] -- AndyW, Carrie, ...    |
        || |                       +----------------+------------------------------------------------------------------------------------------------------+
        || +CreateNode(8)          |              1 | AndyW, Carrie, Hugo, JoelS, Keanu, LanaW, Laurence, TheMatrix                                        |
        |+-------------------------+----------------+------------------------------------------------------------------------------------------------------+
        |""".stripMargin

    val result = executeWith(expectedToSucceed, query,
      planComparisonStrategy = ComparePlansWithAssertion(_ should matchPlan(expectedPlan), expectPlansToFail = Configs.AllRulePlanners))
    assertStats(result, nodesCreated = 171, relationshipsCreated = 253, propertiesWritten = 564, labelsAdded = 171)
  }

  test("Compact smaller, but still long and compactable query"){
    val query = """CREATE (TheMatrix:Movie {title:'The Matrix', released:2001, tagline:'Welcome to the Real World3'})
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
        |""".stripMargin

    // Removed produceResults part of plan which differs between scenarios and isn't what we want to test
    val expectedPlan =
      """
        || |                      +----------------+---------------------------------------------------------------------------------------------------+
        || +CreateRelationship(7) |              1 | anon[517], anon[570], anon[629], anon[685], anon[745], anon[781], anon[817] -- AndyW, Carrie, ... |
        || |                      +----------------+---------------------------------------------------------------------------------------------------+
        || +CreateNode(8)         |              1 | AndyW, Carrie, Hugo, JoelS, Keanu, LanaW, Laurence, TheMatrix                                     |
        |+------------------------+----------------+---------------------------------------------------------------------------------------------------+
        |""".stripMargin

    val result = executeWith(expectedToSucceed, query, planComparisonStrategy = ComparePlansWithAssertion(_ should matchPlan(expectedPlan), expectPlansToFail = Configs.AllRulePlanners))
    assertStats(result, nodesCreated = 8, relationshipsCreated = 7, propertiesWritten = 21, labelsAdded = 8)
  }

  test("Don't compact complex query") {
    val query = "EXPLAIN LOAD CSV WITH HEADERS FROM {csv_filename} AS line MERGE (u1:User {login: line.user1}) MERGE " +
      "(u2:User {login: line.user2}) CREATE (u1)-[:FRIEND]->(u2)"
    val expectedPlan =
      """
        |+-------------------------+----------------+---------------------------+-----------------------+
        || Operator                | Estimated Rows | Variables                 | Other                 |
        |+-------------------------+----------------+---------------------------+-----------------------+
        || +ProduceResults         |              1 | anon[134], line, u1, u2   |                       |
        || |                       +----------------+---------------------------+-----------------------+
        || +EmptyResult            |              1 | anon[134], line, u1, u2   |                       |
        || |                       +----------------+---------------------------+-----------------------+
        || +CreateRelationship     |              1 | anon[134] -- line, u1, u2 |                       |
        || |                       +----------------+---------------------------+-----------------------+
        || +Apply                  |              1 | u1 -- line, u2            |                       |
        || |\                      +----------------+---------------------------+-----------------------+
        || | +AntiConditionalApply |              1 | line, u2                  |                       |
        || | |\                    +----------------+---------------------------+-----------------------+
        || | | +MergeCreateNode    |              1 | u2 -- line                |                       |
        || | | |                   +----------------+---------------------------+-----------------------+
        || | | +Argument           |              1 | line                      |                       |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +Optional             |              1 | line, u2                  |                       |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +ActiveRead           |              1 | line, u2                  |                       |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +Filter               |              0 | line, u2                  | u2.login = line.user2 |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +NodeByLabelScan      |              1 | u2 -- line                | :User                 |
        || |                       +----------------+---------------------------+-----------------------+
        || +Eager                  |              1 | line, u1                  |                       |
        || |                       +----------------+---------------------------+-----------------------+
        || +Apply                  |              1 | line, u1                  |                       |
        || |\                      +----------------+---------------------------+-----------------------+
        || | +AntiConditionalApply |              1 | line, u1                  |                       |
        || | |\                    +----------------+---------------------------+-----------------------+
        || | | +MergeCreateNode    |              1 | u1 -- line                |                       |
        || | | |                   +----------------+---------------------------+-----------------------+
        || | | +Argument           |              1 | line                      |                       |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +Optional             |              1 | line, u1                  |                       |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +ActiveRead           |              1 | line, u1                  |                       |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +Filter               |              0 | line, u1                  | u1.login = line.user1 |
        || | |                     +----------------+---------------------------+-----------------------+
        || | +NodeByLabelScan      |              1 | u1 -- line                | :User                 |
        || |                       +----------------+---------------------------+-----------------------+
        || +LoadCSV                |              1 | line                      |                       |
        |+-------------------------+----------------+---------------------------+-----------------------+
        |""".stripMargin
    executeWith(expectedToSucceed, query, planComparisonStrategy = ComparePlansWithAssertion(_ should matchPlan(expectedPlan),
      expectPlansToFail = Configs.All - Configs.Version3_3 - Configs.Cost3_4 - Configs.DefaultInterpreted), params = Map("csv_filename" -> "x"))
  }

  test("Don't compact query with consecutive expands due to presence of values in 'other' column") {
    val a = createLabeledNode(Map("name"->"Keanu Reeves"), "Actor")
    val b = createLabeledNode(Map("name"->"Craig"), "Actor")
    val c = createLabeledNode(Map("name"->"Olivia"), "Actor")
    val d = createLabeledNode(Map("name"->"Carrie"), "Actor")
    val e = createLabeledNode(Map("name"->"Andres"), "Actor")
    relate(a,b)
    relate(b,c)
    relate(c,d)
    relate(d,e)
    relate(c,b)
    relate(d,b)
    val query = "MATCH (n:Actor {name:'Keanu Reeves'})-->()-->(b) RETURN b"
    val expectedPlan =
      """+------------------+----------------+--------------------------------------+-----------------------------+
        || Operator         | Estimated Rows | Variables                            | Other                       |
        |+------------------+----------------+--------------------------------------+-----------------------------+
        || +ProduceResults  |              1 | anon[38], anon[41], anon[43], b, n   |                             |
        || |                +----------------+--------------------------------------+-----------------------------+
        || +Filter          |              1 | anon[38], anon[41], anon[43], b, n   | not `anon[38]` = `anon[43]` |
        || |                +----------------+--------------------------------------+-----------------------------+
        || +Expand(All)     |              1 | anon[43], b -- anon[38], anon[41], n | ()-->(b)                    |
        || |                +----------------+--------------------------------------+-----------------------------+
        || +Expand(All)     |              1 | anon[38], anon[41] -- n              | (n)-->()                    |
        || |                +----------------+--------------------------------------+-----------------------------+
        || +Filter          |              1 | n                                    | n.name = $`  AUTOSTRING0`   |
        || |                +----------------+--------------------------------------+-----------------------------+
        || +NodeByLabelScan |              5 | n                                    | :Actor                      |
        |+------------------+----------------+--------------------------------------+-----------------------------+
        |""".stripMargin
    val ignoreConfiguration = Configs.Version2_3 + Configs.Version3_1 + Configs.AllRulePlanners + Configs.SlottedInterpreted
    executeWith(Configs.All, query, planComparisonStrategy = ComparePlansWithAssertion(_ should matchPlan(expectedPlan), expectPlansToFail = ignoreConfiguration))
  }

  test("plans are alike with different anon variable numbers") {
    val plan1 =
      """
        |+------------------+----------------+------+---------+--------------------------------------+---------------------------+
        || Operator         | Estimated Rows | Rows | DB Hits | Variables                            | Other                     |
        |+------------------+----------------+------+---------+--------------------------------------+---------------------------+
        || +ProduceResults  |              1 |    1 |       0 | b                                    | b                         |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Filter          |              1 |    1 |       0 | anon[00], anon[11], anon[22], b, n   | NOT(anon[38] == anon[43]) |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Expand(All)     |              1 |    1 |       2 | anon[33], b -- anon[44], anon[55], n | ()-->(b)                  |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Expand(All)     |              1 |    1 |       2 | anon[66], anon[77] -- n              | (n)-->()                  |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Filter          |              1 |    1 |       5 | n                                    | n.name == {  AUTOSTRING0} |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +NodeByLabelScan |              5 |    5 |       6 | n                                    | :Actor                    |
        |+------------------+----------------+------+---------+--------------------------------------+---------------------------+
      """.stripMargin

    val plan2 =
      """
        |+------------------+----------------+------+---------+--------------------------------------+---------------------------+
        || Operator         | Estimated Rows | Rows | DB Hits | Variables                            | Other                     |
        |+------------------+----------------+------+---------+--------------------------------------+---------------------------+
        || +ProduceResults  |              1 |    1 |       0 | b                                    | b                         |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Filter          |              1 |    1 |       0 | anon[38], anon[41], anon[43], b, n   | NOT(anon[38] == anon[43]) |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Expand(All)     |              1 |    1 |       2 | anon[43], b -- anon[38], anon[41], n | ()-->(b)                  |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Expand(All)     |              1 |    1 |       2 | anon[38], anon[41] -- n              | (n)-->()                  |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +Filter          |              1 |    1 |       5 | n                                    | n.name == {  AUTOSTRING0} |
        || |                +----------------+------+---------+--------------------------------------+---------------------------+
        || +NodeByLabelScan |              5 |    5 |       6 | n                                    | :Actor                    |
        |+------------------+----------------+------+---------+--------------------------------------+---------------------------+
      """.stripMargin

    replaceAnonVariables(plan1) should be(replaceAnonVariables(plan2))
  }
}
