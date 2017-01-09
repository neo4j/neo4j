/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStoreVersionCheck;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class StoreMigrationIT
{
    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();

    @ClassRule
    public static final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final TestDirectory testDir = TestDirectory.testDirectory( fileSystemRule.get() );

    public static final String CREATE_QUERY =
            "CREATE (TheMatrix:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real " +
            "World'})\n" +
            "CREATE (Keanu:Person {name:'Keanu Reeves', born:1964})\n" +
            "CREATE (Carrie:Person {name:'Carrie-Anne Moss', born:1967})\n" +
            "CREATE (Laurence:Person {name:'Laurence Fishburne', born:1961})\n" +
            "CREATE (Hugo:Person {name:'Hugo Weaving', born:1960})\n" +
            "CREATE (AndyW:Person {name:'Andy Wachowski', born:1967})\n" +
            "CREATE (LanaW:Person {name:'Lana Wachowski', born:1965})\n" +
            "CREATE (JoelS:Person {name:'Joel Silver', born:1952})\n" +
            "CREATE\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrix),\n" +
            "  (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrix),\n" +
            "  (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrix),\n" +
            "  (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrix),\n" +
            "  (AndyW)-[:DIRECTED]->(TheMatrix),\n" +
            "  (LanaW)-[:DIRECTED]->(TheMatrix),\n" +
            "  (JoelS)-[:PRODUCED]->(TheMatrix)\n" +
            "  \n" +
            "CREATE (Emil:Person {name:\"Emil Eifrem\", born:1978})\n" +
            "CREATE (Emil)-[:ACTED_IN {roles:[\"Emil\"]}]->(TheMatrix)\n" +
            "\n" +
            "CREATE (TheMatrixReloaded:Movie {title:'The Matrix Reloaded', released:2003, tagline:'Free" +
            " your mind'})\n" +
            "CREATE\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrixReloaded),\n" +
            "  (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrixReloaded),\n" +
            "  (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrixReloaded),\n" +
            "  (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrixReloaded),\n" +
            "  (AndyW)-[:DIRECTED]->(TheMatrixReloaded),\n" +
            "  (LanaW)-[:DIRECTED]->(TheMatrixReloaded),\n" +
            "  (JoelS)-[:PRODUCED]->(TheMatrixReloaded)\n" +
            "  \n" +
            "CREATE (TheMatrixRevolutions:Movie {title:'The Matrix Revolutions', released:2003, " +
            "tagline:'Everything that has a beginning has an end'})\n" +
            "CREATE\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Neo']}]->(TheMatrixRevolutions),\n" +
            "  (Carrie)-[:ACTED_IN {roles:['Trinity']}]->(TheMatrixRevolutions),\n" +
            "  (Laurence)-[:ACTED_IN {roles:['Morpheus']}]->(TheMatrixRevolutions),\n" +
            "  (Hugo)-[:ACTED_IN {roles:['Agent Smith']}]->(TheMatrixRevolutions),\n" +
            "  (AndyW)-[:DIRECTED]->(TheMatrixRevolutions),\n" +
            "  (LanaW)-[:DIRECTED]->(TheMatrixRevolutions),\n" +
            "  (JoelS)-[:PRODUCED]->(TheMatrixRevolutions)\n" +
            "  \n" +
            "CREATE (TheDevilsAdvocate:Movie {title:\"The Devil's Advocate\", released:1997, " +
            "tagline:'Evil has its winning ways'})\n" +
            "CREATE (Charlize:Person {name:'Charlize Theron', born:1975})\n" +
            "CREATE (Al:Person {name:'Al Pacino', born:1940})\n" +
            "CREATE (Taylor:Person {name:'Taylor Hackford', born:1944})\n" +
            "CREATE\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Kevin Lomax']}]->(TheDevilsAdvocate),\n" +
            "  (Charlize)-[:ACTED_IN {roles:['Mary Ann Lomax']}]->(TheDevilsAdvocate),\n" +
            "  (Al)-[:ACTED_IN {roles:['John Milton']}]->(TheDevilsAdvocate),\n" +
            "  (Taylor)-[:DIRECTED]->(TheDevilsAdvocate)\n" +
            "  \n" +
            "CREATE (AFewGoodMen:Movie {title:\"A Few Good Men\", released:1992, tagline:\"In the heart" +
            " of the nation's capital, in a courthouse of the U.S. government, one man will stop at " +
            "nothing to keep his honor, and one will stop at nothing to find the truth.\"})\n" +
            "CREATE (TomC:Person {name:'Tom Cruise', born:1962})\n" +
            "CREATE (JackN:Person {name:'Jack Nicholson', born:1937})\n" +
            "CREATE (DemiM:Person {name:'Demi Moore', born:1962})\n" +
            "CREATE (KevinB:Person {name:'Kevin Bacon', born:1958})\n" +
            "CREATE (KieferS:Person {name:'Kiefer Sutherland', born:1966})\n" +
            "CREATE (NoahW:Person {name:'Noah Wyle', born:1971})\n" +
            "CREATE (CubaG:Person {name:'Cuba Gooding Jr.', born:1968})\n" +
            "CREATE (KevinP:Person {name:'Kevin Pollak', born:1957})\n" +
            "CREATE (JTW:Person {name:'J.T. Walsh', born:1943})\n" +
            "CREATE (JamesM:Person {name:'James Marshall', born:1967})\n" +
            "CREATE (ChristopherG:Person {name:'Christopher Guest', born:1948})\n" +
            "CREATE (RobR:Person {name:'Rob Reiner', born:1947})\n" +
            "CREATE (AaronS:Person {name:'Aaron Sorkin', born:1961})\n" +
            "CREATE\n" +
            "  (TomC)-[:ACTED_IN {roles:['Lt. Daniel Kaffee']}]->(AFewGoodMen),\n" +
            "  (JackN)-[:ACTED_IN {roles:['Col. Nathan R. Jessup']}]->(AFewGoodMen),\n" +
            "  (DemiM)-[:ACTED_IN {roles:['Lt. Cdr. JoAnne Galloway']}]->(AFewGoodMen),\n" +
            "  (KevinB)-[:ACTED_IN {roles:['Capt. Jack Ross']}]->(AFewGoodMen),\n" +
            "  (KieferS)-[:ACTED_IN {roles:['Lt. Jonathan Kendrick']}]->(AFewGoodMen),\n" +
            "  (NoahW)-[:ACTED_IN {roles:['Cpl. Jeffrey Barnes']}]->(AFewGoodMen),\n" +
            "  (CubaG)-[:ACTED_IN {roles:['Cpl. Carl Hammaker']}]->(AFewGoodMen),\n" +
            "  (KevinP)-[:ACTED_IN {roles:['Lt. Sam Weinberg']}]->(AFewGoodMen),\n" +
            "  (JTW)-[:ACTED_IN {roles:['Lt. Col. Matthew Andrew Markinson']}]->(AFewGoodMen),\n" +
            "  (JamesM)-[:ACTED_IN {roles:['Pfc. Louden Downey']}]->(AFewGoodMen),\n" +
            "  (ChristopherG)-[:ACTED_IN {roles:['Dr. Stone']}]->(AFewGoodMen),\n" +
            "  (AaronS)-[:ACTED_IN {roles:['Man in Bar']}]->(AFewGoodMen),\n" +
            "  (RobR)-[:DIRECTED]->(AFewGoodMen),\n" +
            "  (AaronS)-[:WROTE]->(AFewGoodMen)\n" +
            "  \n" +
            "CREATE (TopGun:Movie {title:\"Top Gun\", released:1986, tagline:'I feel the need, the need" +
            " for speed.'})\n" +
            "CREATE (KellyM:Person {name:'Kelly McGillis', born:1957})\n" +
            "CREATE (ValK:Person {name:'Val Kilmer', born:1959})\n" +
            "CREATE (AnthonyE:Person {name:'Anthony Edwards', born:1962})\n" +
            "CREATE (TomS:Person {name:'Tom Skerritt', born:1933})\n" +
            "CREATE (MegR:Person {name:'Meg Ryan', born:1961})\n" +
            "CREATE (TonyS:Person {name:'Tony Scott', born:1944})\n" +
            "CREATE (JimC:Person {name:'Jim Cash', born:1941})\n" +
            "CREATE\n" +
            "  (TomC)-[:ACTED_IN {roles:['Maverick']}]->(TopGun),\n" +
            "  (KellyM)-[:ACTED_IN {roles:['Charlie']}]->(TopGun),\n" +
            "  (ValK)-[:ACTED_IN {roles:['Iceman']}]->(TopGun),\n" +
            "  (AnthonyE)-[:ACTED_IN {roles:['Goose']}]->(TopGun),\n" +
            "  (TomS)-[:ACTED_IN {roles:['Viper']}]->(TopGun),\n" +
            "  (MegR)-[:ACTED_IN {roles:['Carole']}]->(TopGun),\n" +
            "  (TonyS)-[:DIRECTED]->(TopGun),\n" +
            "  (JimC)-[:WROTE]->(TopGun)\n" +
            "  \n" +
            "CREATE (JerryMaguire:Movie {title:'Jerry Maguire', released:2000, tagline:'The rest of his" +
            " life begins now.'})\n" +
            "CREATE (ReneeZ:Person {name:'Renee Zellweger', born:1969})\n" +
            "CREATE (KellyP:Person {name:'Kelly Preston', born:1962})\n" +
            "CREATE (JerryO:Person {name:\"Jerry O'Connell\", born:1974})\n" +
            "CREATE (JayM:Person {name:'Jay Mohr', born:1970})\n" +
            "CREATE (BonnieH:Person {name:'Bonnie Hunt', born:1961})\n" +
            "CREATE (ReginaK:Person {name:'Regina King', born:1971})\n" +
            "CREATE (JonathanL:Person {name:'Jonathan Lipnicki', born:1996})\n" +
            "CREATE (CameronC:Person {name:'Cameron Crowe', born:1957})\n" +
            "CREATE\n" +
            "  (TomC)-[:ACTED_IN {roles:['Jerry Maguire']}]->(JerryMaguire),\n" +
            "  (CubaG)-[:ACTED_IN {roles:['Rod Tidwell']}]->(JerryMaguire),\n" +
            "  (ReneeZ)-[:ACTED_IN {roles:['Dorothy Boyd']}]->(JerryMaguire),\n" +
            "  (KellyP)-[:ACTED_IN {roles:['Avery Bishop']}]->(JerryMaguire),\n" +
            "  (JerryO)-[:ACTED_IN {roles:['Frank Cushman']}]->(JerryMaguire),\n" +
            "  (JayM)-[:ACTED_IN {roles:['Bob Sugar']}]->(JerryMaguire),\n" +
            "  (BonnieH)-[:ACTED_IN {roles:['Laurel Boyd']}]->(JerryMaguire),\n" +
            "  (ReginaK)-[:ACTED_IN {roles:['Marcee Tidwell']}]->(JerryMaguire),\n" +
            "  (JonathanL)-[:ACTED_IN {roles:['Ray Boyd']}]->(JerryMaguire),\n" +
            "  (CameronC)-[:DIRECTED]->(JerryMaguire),\n" +
            "  (CameronC)-[:PRODUCED]->(JerryMaguire),\n" +
            "  (CameronC)-[:WROTE]->(JerryMaguire)\n" +
            "  \n" +
            "CREATE (StandByMe:Movie {title:\"Stand By Me\", released:1986, tagline:\"For some, it's " +
            "the last real taste of innocence, and the first real taste of life. But for everyone, it's" +
            " the time that memories are made of.\"})\n" +
            "CREATE (RiverP:Person {name:'River Phoenix', born:1970})\n" +
            "CREATE (CoreyF:Person {name:'Corey Feldman', born:1971})\n" +
            "CREATE (WilW:Person {name:'Wil Wheaton', born:1972})\n" +
            "CREATE (JohnC:Person {name:'John Cusack', born:1966})\n" +
            "CREATE (MarshallB:Person {name:'Marshall Bell', born:1942})\n" +
            "CREATE\n" +
            "  (WilW)-[:ACTED_IN {roles:['Gordie Lachance']}]->(StandByMe),\n" +
            "  (RiverP)-[:ACTED_IN {roles:['Chris Chambers']}]->(StandByMe),\n" +
            "  (JerryO)-[:ACTED_IN {roles:['Vern Tessio']}]->(StandByMe),\n" +
            "  (CoreyF)-[:ACTED_IN {roles:['Teddy Duchamp']}]->(StandByMe),\n" +
            "  (JohnC)-[:ACTED_IN {roles:['Denny Lachance']}]->(StandByMe),\n" +
            "  (KieferS)-[:ACTED_IN {roles:['Ace Merrill']}]->(StandByMe),\n" +
            "  (MarshallB)-[:ACTED_IN {roles:['Mr. Lachance']}]->(StandByMe),\n" +
            "  (RobR)-[:DIRECTED]->(StandByMe)\n" +
            "  \n" +
            "CREATE (AsGoodAsItGets:Movie {title:'As Good as It Gets', released:1997, tagline:'A comedy" +
            " from the heart that goes for the throat.'})\n" +
            "CREATE (HelenH:Person {name:'Helen Hunt', born:1963})\n" +
            "CREATE (GregK:Person {name:'Greg Kinnear', born:1963})\n" +
            "CREATE (JamesB:Person {name:'James L. Brooks', born:1940})\n" +
            "CREATE\n" +
            "  (JackN)-[:ACTED_IN {roles:['Melvin Udall']}]->(AsGoodAsItGets),\n" +
            "  (HelenH)-[:ACTED_IN {roles:['Carol Connelly']}]->(AsGoodAsItGets),\n" +
            "  (GregK)-[:ACTED_IN {roles:['Simon Bishop']}]->(AsGoodAsItGets),\n" +
            "  (CubaG)-[:ACTED_IN {roles:['Frank Sachs']}]->(AsGoodAsItGets),\n" +
            "  (JamesB)-[:DIRECTED]->(AsGoodAsItGets)\n" +
            "  \n" +
            "CREATE (WhatDreamsMayCome:Movie {title:'What Dreams May Come', released:1998, " +
            "tagline:'After life there is more. The end is just the beginning.'})\n" +
            "CREATE (AnnabellaS:Person {name:'Annabella Sciorra', born:1960})\n" +
            "CREATE (MaxS:Person {name:'Max von Sydow', born:1929})\n" +
            "CREATE (WernerH:Person {name:'Werner Herzog', born:1942})\n" +
            "CREATE (Robin:Person {name:'Robin Williams', born:1951})\n" +
            "CREATE (VincentW:Person {name:'Vincent Ward', born:1956})\n" +
            "CREATE\n" +
            "  (Robin)-[:ACTED_IN {roles:['Chris Nielsen']}]->(WhatDreamsMayCome),\n" +
            "  (CubaG)-[:ACTED_IN {roles:['Albert Lewis']}]->(WhatDreamsMayCome),\n" +
            "  (AnnabellaS)-[:ACTED_IN {roles:['Annie Collins-Nielsen']}]->(WhatDreamsMayCome),\n" +
            "  (MaxS)-[:ACTED_IN {roles:['The Tracker']}]->(WhatDreamsMayCome),\n" +
            "  (WernerH)-[:ACTED_IN {roles:['The Face']}]->(WhatDreamsMayCome),\n" +
            "  (VincentW)-[:DIRECTED]->(WhatDreamsMayCome)\n" +
            "  \n" +
            "CREATE (SnowFallingonCedars:Movie {title:'Snow Falling on Cedars', released:1999, " +
            "tagline:'First loves last. Forever.'})\n" +
            "CREATE (EthanH:Person {name:'Ethan Hawke', born:1970})\n" +
            "CREATE (RickY:Person {name:'Rick Yune', born:1971})\n" +
            "CREATE (JamesC:Person {name:'James Cromwell', born:1940})\n" +
            "CREATE (ScottH:Person {name:'Scott Hicks', born:1953})\n" +
            "CREATE\n" +
            "  (EthanH)-[:ACTED_IN {roles:['Ishmael Chambers']}]->(SnowFallingonCedars),\n" +
            "  (RickY)-[:ACTED_IN {roles:['Kazuo Miyamoto']}]->(SnowFallingonCedars),\n" +
            "  (MaxS)-[:ACTED_IN {roles:['Nels Gudmundsson']}]->(SnowFallingonCedars),\n" +
            "  (JamesC)-[:ACTED_IN {roles:['Judge Fielding']}]->(SnowFallingonCedars),\n" +
            "  (ScottH)-[:DIRECTED]->(SnowFallingonCedars)\n" +
            "  \n" +
            "CREATE (YouveGotMail:Movie {title:\"You've Got Mail\", released:1998, tagline:'At odds in " +
            "life... in love on-line.'})\n" +
            "CREATE (ParkerP:Person {name:'Parker Posey', born:1968})\n" +
            "CREATE (DaveC:Person {name:'Dave Chappelle', born:1973})\n" +
            "CREATE (SteveZ:Person {name:'Steve Zahn', born:1967})\n" +
            "CREATE (TomH:Person {name:'Tom Hanks', born:1956})\n" +
            "CREATE (NoraE:Person {name:'Nora Ephron', born:1941})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Joe Fox']}]->(YouveGotMail),\n" +
            "  (MegR)-[:ACTED_IN {roles:['Kathleen Kelly']}]->(YouveGotMail),\n" +
            "  (GregK)-[:ACTED_IN {roles:['Frank Navasky']}]->(YouveGotMail),\n" +
            "  (ParkerP)-[:ACTED_IN {roles:['Patricia Eden']}]->(YouveGotMail),\n" +
            "  (DaveC)-[:ACTED_IN {roles:['Kevin Jackson']}]->(YouveGotMail),\n" +
            "  (SteveZ)-[:ACTED_IN {roles:['George Pappas']}]->(YouveGotMail),\n" +
            "  (NoraE)-[:DIRECTED]->(YouveGotMail)\n" +
            "  \n" +
            "CREATE (SleeplessInSeattle:Movie {title:'Sleepless in Seattle', released:1993, " +
            "tagline:'What if someone you never met, someone you never saw, someone you never knew was " +
            "the only someone for you?'})\n" +
            "CREATE (RitaW:Person {name:'Rita Wilson', born:1956})\n" +
            "CREATE (BillPull:Person {name:'Bill Pullman', born:1953})\n" +
            "CREATE (VictorG:Person {name:'Victor Garber', born:1949})\n" +
            "CREATE (RosieO:Person {name:\"Rosie O'Donnell\", born:1962})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Sam Baldwin']}]->(SleeplessInSeattle),\n" +
            "  (MegR)-[:ACTED_IN {roles:['Annie Reed']}]->(SleeplessInSeattle),\n" +
            "  (RitaW)-[:ACTED_IN {roles:['Suzy']}]->(SleeplessInSeattle),\n" +
            "  (BillPull)-[:ACTED_IN {roles:['Walter']}]->(SleeplessInSeattle),\n" +
            "  (VictorG)-[:ACTED_IN {roles:['Greg']}]->(SleeplessInSeattle),\n" +
            "  (RosieO)-[:ACTED_IN {roles:['Becky']}]->(SleeplessInSeattle),\n" +
            "  (NoraE)-[:DIRECTED]->(SleeplessInSeattle)\n" +
            "  \n" +
            "CREATE (JoeVersustheVolcano:Movie {title:'Joe Versus the Volcano', released:1990, " +
            "tagline:'A story of love, lava and burning desire.'})\n" +
            "CREATE (JohnS:Person {name:'John Patrick Stanley', born:1950})\n" +
            "CREATE (Nathan:Person {name:'Nathan Lane', born:1956})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Joe Banks']}]->(JoeVersustheVolcano),\n" +
            "  (MegR)-[:ACTED_IN {roles:['DeDe', 'Angelica Graynamore', 'Patricia Graynamore']}]->" +
            "(JoeVersustheVolcano),\n" +
            "  (Nathan)-[:ACTED_IN {roles:['Baw']}]->(JoeVersustheVolcano),\n" +
            "  (JohnS)-[:DIRECTED]->(JoeVersustheVolcano)\n" +
            "  \n" +
            "CREATE (WhenHarryMetSally:Movie {title:'When Harry Met Sally', released:1998, tagline:'At " +
            "odds in life... in love on-line.'})\n" +
            "CREATE (BillyC:Person {name:'Billy Crystal', born:1948})\n" +
            "CREATE (CarrieF:Person {name:'Carrie Fisher', born:1956})\n" +
            "CREATE (BrunoK:Person {name:'Bruno Kirby', born:1949})\n" +
            "CREATE\n" +
            "  (BillyC)-[:ACTED_IN {roles:['Harry Burns']}]->(WhenHarryMetSally),\n" +
            "  (MegR)-[:ACTED_IN {roles:['Sally Albright']}]->(WhenHarryMetSally),\n" +
            "  (CarrieF)-[:ACTED_IN {roles:['Marie']}]->(WhenHarryMetSally),\n" +
            "  (BrunoK)-[:ACTED_IN {roles:['Jess']}]->(WhenHarryMetSally),\n" +
            "  (RobR)-[:DIRECTED]->(WhenHarryMetSally),\n" +
            "  (RobR)-[:PRODUCED]->(WhenHarryMetSally),\n" +
            "  (NoraE)-[:PRODUCED]->(WhenHarryMetSally),\n" +
            "  (NoraE)-[:WROTE]->(WhenHarryMetSally)\n" +
            "  \n" +
            "CREATE (ThatThingYouDo:Movie {title:'That Thing You Do', released:1996, tagline:'In every " +
            "life there comes a time when that thing you dream becomes that thing you do'})\n" +
            "CREATE (LivT:Person {name:'Liv Tyler', born:1977})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Mr. White']}]->(ThatThingYouDo),\n" +
            "  (LivT)-[:ACTED_IN {roles:['Faye Dolan']}]->(ThatThingYouDo),\n" +
            "  (Charlize)-[:ACTED_IN {roles:['Tina']}]->(ThatThingYouDo),\n" +
            "  (TomH)-[:DIRECTED]->(ThatThingYouDo)\n" +
            "  \n" +
            "CREATE (TheReplacements:Movie {title:'The Replacements', released:2000, tagline:'Pain " +
            "heals, Chicks dig scars... Glory lasts forever'})\n" +
            "CREATE (Brooke:Person {name:'Brooke Langton', born:1970})\n" +
            "CREATE (Gene:Person {name:'Gene Hackman', born:1930})\n" +
            "CREATE (Orlando:Person {name:'Orlando Jones', born:1968})\n" +
            "CREATE (Howard:Person {name:'Howard Deutch', born:1950})\n" +
            "CREATE\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Shane Falco']}]->(TheReplacements),\n" +
            "  (Brooke)-[:ACTED_IN {roles:['Annabelle Farrell']}]->(TheReplacements),\n" +
            "  (Gene)-[:ACTED_IN {roles:['Jimmy McGinty']}]->(TheReplacements),\n" +
            "  (Orlando)-[:ACTED_IN {roles:['Clifford Franklin']}]->(TheReplacements),\n" +
            "  (Howard)-[:DIRECTED]->(TheReplacements)\n" +
            "  \n" +
            "CREATE (RescueDawn:Movie {title:'RescueDawn', released:2006, tagline:\"Based on the " +
            "extraordinary true story of one man's fight for freedom\"})\n" +
            "CREATE (ChristianB:Person {name:'Christian Bale', born:1974})\n" +
            "CREATE (ZachG:Person {name:'Zach Grenier', born:1954})\n" +
            "CREATE\n" +
            "  (MarshallB)-[:ACTED_IN {roles:['Admiral']}]->(RescueDawn),\n" +
            "  (ChristianB)-[:ACTED_IN {roles:['Dieter Dengler']}]->(RescueDawn),\n" +
            "  (ZachG)-[:ACTED_IN {roles:['Squad Leader']}]->(RescueDawn),\n" +
            "  (SteveZ)-[:ACTED_IN {roles:['Duane']}]->(RescueDawn),\n" +
            "  (WernerH)-[:DIRECTED]->(RescueDawn)\n" +
            "  \n" +
            "CREATE (TheBirdcage:Movie {title:'The Birdcage', released:1996, tagline:'Come as you " +
            "are'})\n" +
            "CREATE (MikeN:Person {name:'Mike Nichols', born:1931})\n" +
            "CREATE\n" +
            "  (Robin)-[:ACTED_IN {roles:['Armand Goldman']}]->(TheBirdcage),\n" +
            "  (Nathan)-[:ACTED_IN {roles:['Albert Goldman']}]->(TheBirdcage),\n" +
            "  (Gene)-[:ACTED_IN {roles:['Sen. Kevin Keeley']}]->(TheBirdcage),\n" +
            "  (MikeN)-[:DIRECTED]->(TheBirdcage)\n" +
            "  \n" +
            "CREATE (Unforgiven:Movie {title:'Unforgiven', released:1992, tagline:\"It's a hell of a " +
            "thing, killing a man\"})\n" +
            "CREATE (RichardH:Person {name:'Richard Harris', born:1930})\n" +
            "CREATE (ClintE:Person {name:'Clint Eastwood', born:1930})\n" +
            "CREATE\n" +
            "  (RichardH)-[:ACTED_IN {roles:['English Bob']}]->(Unforgiven),\n" +
            "  (ClintE)-[:ACTED_IN {roles:['Bill Munny']}]->(Unforgiven),\n" +
            "  (Gene)-[:ACTED_IN {roles:['Little Bill Daggett']}]->(Unforgiven),\n" +
            "  (ClintE)-[:DIRECTED]->(Unforgiven)\n" +
            "  \n" +
            "CREATE (JohnnyMnemonic:Movie {title:'Johnny Mnemonic', released:1995, tagline:'The hottest" +
            " data on earth. In the coolest head in town'})\n" +
            "CREATE (Takeshi:Person {name:'Takeshi Kitano', born:1947})\n" +
            "CREATE (Dina:Person {name:'Dina Meyer', born:1968})\n" +
            "CREATE (IceT:Person {name:'Ice-T', born:1958})\n" +
            "CREATE (RobertL:Person {name:'Robert Longo', born:1953})\n" +
            "CREATE\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Johnny Mnemonic']}]->(JohnnyMnemonic),\n" +
            "  (Takeshi)-[:ACTED_IN {roles:['Takahashi']}]->(JohnnyMnemonic),\n" +
            "  (Dina)-[:ACTED_IN {roles:['Jane']}]->(JohnnyMnemonic),\n" +
            "  (IceT)-[:ACTED_IN {roles:['J-Bone']}]->(JohnnyMnemonic),\n" +
            "  (RobertL)-[:DIRECTED]->(JohnnyMnemonic)\n" +
            "  \n" +
            "CREATE (CloudAtlas:Movie {title:'Cloud Atlas', released:2012, tagline:'Everything is " +
            "connected'})\n" +
            "CREATE (HalleB:Person {name:'Halle Berry', born:1966})\n" +
            "CREATE (JimB:Person {name:'Jim Broadbent', born:1949})\n" +
            "CREATE (TomT:Person {name:'Tom Tykwer', born:1965})\n" +
            "CREATE (DavidMitchell:Person {name:'David Mitchell', born:1969})\n" +
            "CREATE (StefanArndt:Person {name:'Stefan Arndt', born:1961})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Zachry', 'Dr. Henry Goose', 'Isaac Sachs', 'Dermot " +
            "Hoggins']}]->(CloudAtlas),\n" +
            "  (Hugo)-[:ACTED_IN {roles:['Bill Smoke', 'Haskell Moore', 'Tadeusz Kesselring', 'Nurse " +
            "Noakes', 'Boardman Mephi', 'Old Georgie']}]->(CloudAtlas),\n" +
            "  (HalleB)-[:ACTED_IN {roles:['Luisa Rey', 'Jocasta Ayrs', 'Ovid', 'Meronym']}]->" +
            "(CloudAtlas),\n" +
            "  (JimB)-[:ACTED_IN {roles:['Vyvyan Ayrs', 'Captain Molyneux', 'Timothy Cavendish']}]->" +
            "(CloudAtlas),\n" +
            "  (TomT)-[:DIRECTED]->(CloudAtlas),\n" +
            "  (AndyW)-[:DIRECTED]->(CloudAtlas),\n" +
            "  (LanaW)-[:DIRECTED]->(CloudAtlas),\n" +
            "  (DavidMitchell)-[:WROTE]->(CloudAtlas),\n" +
            "  (StefanArndt)-[:PRODUCED]->(CloudAtlas)\n" +
            "  \n" +
            "CREATE (TheDaVinciCode:Movie {title:'The Da Vinci Code', released:2006, tagline:'Break The" +
            " Codes'})\n" +
            "CREATE (IanM:Person {name:'Ian McKellen', born:1939})\n" +
            "CREATE (AudreyT:Person {name:'Audrey Tautou', born:1976})\n" +
            "CREATE (PaulB:Person {name:'Paul Bettany', born:1971})\n" +
            "CREATE (RonH:Person {name:'Ron Howard', born:1954})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Dr. Robert Langdon']}]->(TheDaVinciCode),\n" +
            "  (IanM)-[:ACTED_IN {roles:['Sir Leight Teabing']}]->(TheDaVinciCode),\n" +
            "  (AudreyT)-[:ACTED_IN {roles:['Sophie Neveu']}]->(TheDaVinciCode),\n" +
            "  (PaulB)-[:ACTED_IN {roles:['Silas']}]->(TheDaVinciCode),\n" +
            "  (RonH)-[:DIRECTED]->(TheDaVinciCode)\n" +
            "  \n" +
            "CREATE (VforVendetta:Movie {title:'V for Vendetta', released:2006, tagline:'Freedom! " +
            "Forever!'})\n" +
            "CREATE (NatalieP:Person {name:'Natalie Portman', born:1981})\n" +
            "CREATE (StephenR:Person {name:'Stephen Rea', born:1946})\n" +
            "CREATE (JohnH:Person {name:'John Hurt', born:1940})\n" +
            "CREATE (BenM:Person {name: 'Ben Miles', born:1967})\n" +
            "CREATE\n" +
            "  (Hugo)-[:ACTED_IN {roles:['V']}]->(VforVendetta),\n" +
            "  (NatalieP)-[:ACTED_IN {roles:['Evey Hammond']}]->(VforVendetta),\n" +
            "  (StephenR)-[:ACTED_IN {roles:['Eric Finch']}]->(VforVendetta),\n" +
            "  (JohnH)-[:ACTED_IN {roles:['High Chancellor Adam Sutler']}]->(VforVendetta),\n" +
            "  (BenM)-[:ACTED_IN {roles:['Dascomb']}]->(VforVendetta),\n" +
            "  (JamesM)-[:DIRECTED]->(VforVendetta),\n" +
            "  (AndyW)-[:PRODUCED]->(VforVendetta),\n" +
            "  (LanaW)-[:PRODUCED]->(VforVendetta),\n" +
            "  (JoelS)-[:PRODUCED]->(VforVendetta),\n" +
            "  (AndyW)-[:WROTE]->(VforVendetta),\n" +
            "  (LanaW)-[:WROTE]->(VforVendetta)\n" +
            "  \n" +
            "CREATE (SpeedRacer:Movie {title:'Speed Racer', released:2008, tagline:'Speed has no " +
            "limits'})\n" +
            "CREATE (EmileH:Person {name:'Emile Hirsch', born:1985})\n" +
            "CREATE (JohnG:Person {name:'John Goodman', born:1960})\n" +
            "CREATE (SusanS:Person {name:'Susan Sarandon', born:1946})\n" +
            "CREATE (MatthewF:Person {name:'Matthew Fox', born:1966})\n" +
            "CREATE (ChristinaR:Person {name:'Christina Ricci', born:1980})\n" +
            "CREATE (Rain:Person {name:'Rain', born:1982})\n" +
            "CREATE\n" +
            "  (EmileH)-[:ACTED_IN {roles:['Speed Racer']}]->(SpeedRacer),\n" +
            "  (JohnG)-[:ACTED_IN {roles:['Pops']}]->(SpeedRacer),\n" +
            "  (SusanS)-[:ACTED_IN {roles:['Mom']}]->(SpeedRacer),\n" +
            "  (MatthewF)-[:ACTED_IN {roles:['Racer X']}]->(SpeedRacer),\n" +
            "  (ChristinaR)-[:ACTED_IN {roles:['Trixie']}]->(SpeedRacer),\n" +
            "  (Rain)-[:ACTED_IN {roles:['Taejo Togokahn']}]->(SpeedRacer),\n" +
            "  (BenM)-[:ACTED_IN {roles:['Cass Jones']}]->(SpeedRacer),\n" +
            "  (AndyW)-[:DIRECTED]->(SpeedRacer),\n" +
            "  (LanaW)-[:DIRECTED]->(SpeedRacer),\n" +
            "  (AndyW)-[:WROTE]->(SpeedRacer),\n" +
            "  (LanaW)-[:WROTE]->(SpeedRacer),\n" +
            "  (JoelS)-[:PRODUCED]->(SpeedRacer)\n" +
            "  \n" +
            "CREATE (NinjaAssassin:Movie {title:'Ninja Assassin', released:2009, tagline:'Prepare to " +
            "enter a secret world of assassins'})\n" +
            "CREATE (NaomieH:Person {name:'Naomie Harris'})\n" +
            "CREATE\n" +
            "  (Rain)-[:ACTED_IN {roles:['Raizo']}]->(NinjaAssassin),\n" +
            "  (NaomieH)-[:ACTED_IN {roles:['Mika Coretti']}]->(NinjaAssassin),\n" +
            "  (RickY)-[:ACTED_IN {roles:['Takeshi']}]->(NinjaAssassin),\n" +
            "  (BenM)-[:ACTED_IN {roles:['Ryan Maslow']}]->(NinjaAssassin),\n" +
            "  (JamesM)-[:DIRECTED]->(NinjaAssassin),\n" +
            "  (AndyW)-[:PRODUCED]->(NinjaAssassin),\n" +
            "  (LanaW)-[:PRODUCED]->(NinjaAssassin),\n" +
            "  (JoelS)-[:PRODUCED]->(NinjaAssassin)\n" +
            "  \n" +
            "CREATE (TheGreenMile:Movie {title:'The Green Mile', released:1999, tagline:\"Walk a mile " +
            "you'll never forget.\"})\n" +
            "CREATE (MichaelD:Person {name:'Michael Clarke Duncan', born:1957})\n" +
            "CREATE (DavidM:Person {name:'David Morse', born:1953})\n" +
            "CREATE (SamR:Person {name:'Sam Rockwell', born:1968})\n" +
            "CREATE (GaryS:Person {name:'Gary Sinise', born:1955})\n" +
            "CREATE (PatriciaC:Person {name:'Patricia Clarkson', born:1959})\n" +
            "CREATE (FrankD:Person {name:'Frank Darabont', born:1959})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Paul Edgecomb']}]->(TheGreenMile),\n" +
            "  (MichaelD)-[:ACTED_IN {roles:['John Coffey']}]->(TheGreenMile),\n" +
            "  (DavidM)-[:ACTED_IN {roles:['Brutus \"Brutal\" Howell']}]->(TheGreenMile),\n" +
            "  (BonnieH)-[:ACTED_IN {roles:['Jan Edgecomb']}]->(TheGreenMile),\n" +
            "  (JamesC)-[:ACTED_IN {roles:['Warden Hal Moores']}]->(TheGreenMile),\n" +
            "  (SamR)-[:ACTED_IN {roles:['\"Wild Bill\" Wharton']}]->(TheGreenMile),\n" +
            "  (GaryS)-[:ACTED_IN {roles:['Burt Hammersmith']}]->(TheGreenMile),\n" +
            "  (PatriciaC)-[:ACTED_IN {roles:['Melinda Moores']}]->(TheGreenMile),\n" +
            "  (FrankD)-[:DIRECTED]->(TheGreenMile)\n" +
            "  \n" +
            "CREATE (FrostNixon:Movie {title:'Frost/Nixon', released:2008, tagline:'400 million people " +
            "were waiting for the truth.'})\n" +
            "CREATE (FrankL:Person {name:'Frank Langella', born:1938})\n" +
            "CREATE (MichaelS:Person {name:'Michael Sheen', born:1969})\n" +
            "CREATE (OliverP:Person {name:'Oliver Platt', born:1960})\n" +
            "CREATE\n" +
            "  (FrankL)-[:ACTED_IN {roles:['Richard Nixon']}]->(FrostNixon),\n" +
            "  (MichaelS)-[:ACTED_IN {roles:['David Frost']}]->(FrostNixon),\n" +
            "  (KevinB)-[:ACTED_IN {roles:['Jack Brennan']}]->(FrostNixon),\n" +
            "  (OliverP)-[:ACTED_IN {roles:['Bob Zelnick']}]->(FrostNixon),\n" +
            "  (SamR)-[:ACTED_IN {roles:['James Reston, Jr.']}]->(FrostNixon),\n" +
            "  (RonH)-[:DIRECTED]->(FrostNixon)\n" +
            "  \n" +
            "CREATE (Hoffa:Movie {title:'Hoffa', released:1992, tagline:\"He didn't want law. He wanted" +
            " justice.\"})\n" +
            "CREATE (DannyD:Person {name:'Danny DeVito', born:1944})\n" +
            "CREATE (JohnR:Person {name:'John C. Reilly', born:1965})\n" +
            "CREATE\n" +
            "  (JackN)-[:ACTED_IN {roles:['Hoffa']}]->(Hoffa),\n" +
            "  (DannyD)-[:ACTED_IN {roles:['Robert \"Bobby\" Ciaro']}]->(Hoffa),\n" +
            "  (JTW)-[:ACTED_IN {roles:['Frank Fitzsimmons']}]->(Hoffa),\n" +
            "  (JohnR)-[:ACTED_IN {roles:['Peter \"Pete\" Connelly']}]->(Hoffa),\n" +
            "  (DannyD)-[:DIRECTED]->(Hoffa)\n" +
            "  \n" +
            "CREATE (Apollo13:Movie {title:'Apollo 13', released:1995, tagline:'Houston, we have a " +
            "problem.'})\n" +
            "CREATE (EdH:Person {name:'Ed Harris', born:1950})\n" +
            "CREATE (BillPax:Person {name:'Bill Paxton', born:1955})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Jim Lovell']}]->(Apollo13),\n" +
            "  (KevinB)-[:ACTED_IN {roles:['Jack Swigert']}]->(Apollo13),\n" +
            "  (EdH)-[:ACTED_IN {roles:['Gene Kranz']}]->(Apollo13),\n" +
            "  (BillPax)-[:ACTED_IN {roles:['Fred Haise']}]->(Apollo13),\n" +
            "  (GaryS)-[:ACTED_IN {roles:['Ken Mattingly']}]->(Apollo13),\n" +
            "  (RonH)-[:DIRECTED]->(Apollo13)\n" +
            "  \n" +
            "CREATE (Twister:Movie {title:'Twister', released:1996, tagline:\"Don't Breathe. Don't Look" +
            " Back.\"})\n" +
            "CREATE (PhilipH:Person {name:'Philip Seymour Hoffman', born:1967})\n" +
            "CREATE (JanB:Person {name:'Jan de Bont', born:1943})\n" +
            "CREATE\n" +
            "  (BillPax)-[:ACTED_IN {roles:['Bill Harding']}]->(Twister),\n" +
            "  (HelenH)-[:ACTED_IN {roles:['Dr. Jo Harding']}]->(Twister),\n" +
            "  (ZachG)-[:ACTED_IN {roles:['Eddie']}]->(Twister),\n" +
            "  (PhilipH)-[:ACTED_IN {roles:['Dustin \"Dusty\" Davis']}]->(Twister),\n" +
            "  (JanB)-[:DIRECTED]->(Twister)\n" +
            "  \n" +
            "CREATE (CastAway:Movie {title:'Cast Away', released:2000, tagline:'At the edge of the " +
            "world, his journey begins.'})\n" +
            "CREATE (RobertZ:Person {name:'Robert Zemeckis', born:1951})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Chuck Noland']}]->(CastAway),\n" +
            "  (HelenH)-[:ACTED_IN {roles:['Kelly Frears']}]->(CastAway),\n" +
            "  (RobertZ)-[:DIRECTED]->(CastAway)\n" +
            "  \n" +
            "CREATE (OneFlewOvertheCuckoosNest:Movie {title:\"One Flew Over the Cuckoo's Nest\", " +
            "released:1975, tagline:\"If he's crazy, what does that make you?\"})\n" +
            "CREATE (MilosF:Person {name:'Milos Forman', born:1932})\n" +
            "CREATE\n" +
            "  (JackN)-[:ACTED_IN {roles:['Randle McMurphy']}]->(OneFlewOvertheCuckoosNest),\n" +
            "  (DannyD)-[:ACTED_IN {roles:['Martini']}]->(OneFlewOvertheCuckoosNest),\n" +
            "  (MilosF)-[:DIRECTED]->(OneFlewOvertheCuckoosNest)\n" +
            "  \n" +
            "CREATE (SomethingsGottaGive:Movie {title:\"Something's Gotta Give\", released:2003})\n" +
            "CREATE (DianeK:Person {name:'Diane Keaton', born:1946})\n" +
            "CREATE (NancyM:Person {name:'Nancy Meyers', born:1949})\n" +
            "CREATE\n" +
            "  (JackN)-[:ACTED_IN {roles:['Harry Sanborn']}]->(SomethingsGottaGive),\n" +
            "  (DianeK)-[:ACTED_IN {roles:['Erica Barry']}]->(SomethingsGottaGive),\n" +
            "  (Keanu)-[:ACTED_IN {roles:['Julian Mercer']}]->(SomethingsGottaGive),\n" +
            "  (NancyM)-[:DIRECTED]->(SomethingsGottaGive),\n" +
            "  (NancyM)-[:PRODUCED]->(SomethingsGottaGive),\n" +
            "  (NancyM)-[:WROTE]->(SomethingsGottaGive)\n" +
            "  \n" +
            "CREATE (BicentennialMan:Movie {title:'Bicentennial Man', released:1999, tagline:\"One " +
            "robot's 200 year journey to become an ordinary man.\"})\n" +
            "CREATE (ChrisC:Person {name:'Chris Columbus', born:1958})\n" +
            "CREATE\n" +
            "  (Robin)-[:ACTED_IN {roles:['Andrew Marin']}]->(BicentennialMan),\n" +
            "  (OliverP)-[:ACTED_IN {roles:['Rupert Burns']}]->(BicentennialMan),\n" +
            "  (ChrisC)-[:DIRECTED]->(BicentennialMan)\n" +
            "  \n" +
            "CREATE (CharlieWilsonsWar:Movie {title:\"Charlie Wilson's War\", released:2007, " +
            "tagline:\"A stiff drink. A little mascara. A lot of nerve. Who said they couldn't bring " +
            "down the Soviet empire.\"})\n" +
            "CREATE (JuliaR:Person {name:'Julia Roberts', born:1967})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Rep. Charlie Wilson']}]->(CharlieWilsonsWar),\n" +
            "  (JuliaR)-[:ACTED_IN {roles:['Joanne Herring']}]->(CharlieWilsonsWar),\n" +
            "  (PhilipH)-[:ACTED_IN {roles:['Gust Avrakotos']}]->(CharlieWilsonsWar),\n" +
            "  (MikeN)-[:DIRECTED]->(CharlieWilsonsWar)\n" +
            "  \n" +
            "CREATE (ThePolarExpress:Movie {title:'The Polar Express', released:2004, tagline:'This " +
            "Holiday Season… Believe'})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Hero Boy', 'Father', 'Conductor', 'Hobo', 'Scrooge', 'Santa " +
            "Claus']}]->(ThePolarExpress),\n" +
            "  (RobertZ)-[:DIRECTED]->(ThePolarExpress)\n" +
            "  \n" +
            "CREATE (ALeagueofTheirOwn:Movie {title:'A League of Their Own', released:1992, " +
            "tagline:'Once in a lifetime you get a chance to do something different.'})\n" +
            "CREATE (Madonna:Person {name:'Madonna', born:1954})\n" +
            "CREATE (GeenaD:Person {name:'Geena Davis', born:1956})\n" +
            "CREATE (LoriP:Person {name:'Lori Petty', born:1963})\n" +
            "CREATE (PennyM:Person {name:'Penny Marshall', born:1943})\n" +
            "CREATE\n" +
            "  (TomH)-[:ACTED_IN {roles:['Jimmy Dugan']}]->(ALeagueofTheirOwn),\n" +
            "  (GeenaD)-[:ACTED_IN {roles:['Dottie Hinson']}]->(ALeagueofTheirOwn),\n" +
            "  (LoriP)-[:ACTED_IN {roles:['Kit Keller']}]->(ALeagueofTheirOwn),\n" +
            "  (RosieO)-[:ACTED_IN {roles:['Doris Murphy']}]->(ALeagueofTheirOwn),\n" +
            "  (Madonna)-[:ACTED_IN {roles:['\"All the Way\" Mae Mordabito']}]->(ALeagueofTheirOwn),\n" +
            "  (BillPax)-[:ACTED_IN {roles:['Bob Hinson']}]->(ALeagueofTheirOwn),\n" +
            "  (PennyM)-[:DIRECTED]->(ALeagueofTheirOwn)\n" +
            "  \n" +
            "CREATE (PaulBlythe:Person {name:'Paul Blythe'})\n" +
            "CREATE (AngelaScope:Person {name:'Angela Scope'})\n" +
            "CREATE (JessicaThompson:Person {name:'Jessica Thompson'})\n" +
            "CREATE (JamesThompson:Person {name:'James Thompson'})\n" +
            "\n" +
            "CREATE\n" +
            "  (JamesThompson)-[:FOLLOWS]->(JessicaThompson),\n" +
            "  (AngelaScope)-[:FOLLOWS]->(JessicaThompson),\n" +
            "  (PaulBlythe)-[:FOLLOWS]->(AngelaScope)\n" +
            "  \n" +
            "CREATE\n" +
            "  (JessicaThompson)-[:REVIEWED {summary:'An amazing journey', rating:95}]->(CloudAtlas),\n" +
            "  (JessicaThompson)-[:REVIEWED {summary:'Silly, but fun', rating:65}]->(TheReplacements)," +
            "\n" +
            "  (JamesThompson)-[:REVIEWED {summary:'The coolest football movie ever', rating:100}]->" +
            "(TheReplacements),\n" +
            "  (AngelaScope)-[:REVIEWED {summary:'Pretty funny at times', rating:62}]->" +
            "(TheReplacements),\n" +
            "  (JessicaThompson)-[:REVIEWED {summary:'Dark, but compelling', rating:85}]->(Unforgiven)," +
            "\n" +
            "  (JessicaThompson)-[:REVIEWED {summary:\"Slapstick redeemed only by the Robin Williams " +
            "and Gene Hackman's stellar performances\", rating:45}]->(TheBirdcage),\n" +
            "  (JessicaThompson)-[:REVIEWED {summary:'A solid romp', rating:68}]->(TheDaVinciCode),\n" +
            "  (JamesThompson)-[:REVIEWED {summary:'Fun, but a little far fetched', rating:65}]->" +
            "(TheDaVinciCode),\n" +
            "  (JessicaThompson)-[:REVIEWED {summary:'You had me at Jerry', rating:92}]->(JerryMaguire)" +
            "\n" +
            "  \n" +
            "WITH TomH as a\n" +
            "MATCH (a)-[:ACTED_IN]->(m)<-[:DIRECTED]-(d) RETURN a,m,d LIMIT 10\n" +
            ";";
    protected final RecordFormats from;
    protected final RecordFormats to;

    @Parameterized.Parameters( name = "Migrate: {0}->{1}" )
    public static Iterable<Object[]> data() throws IOException
    {
        FileSystemAbstraction fs = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        File dir = TestDirectory.testDirectory( StoreMigrationIT.class ).prepareDirectoryForTest( "migration" );
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( pageCache );
        LegacyStoreVersionCheck legacyStoreVersionCheck = new LegacyStoreVersionCheck( fs );
        List<Object[]> data = new ArrayList<>();
        ArrayList<RecordFormats> recordFormatses = new ArrayList<>();
        RecordFormatSelector.allFormats().forEach( ( f ) -> addIfNotThere( f, recordFormatses ) );
        for ( RecordFormats toFormat : recordFormatses )
        {
            UpgradableDatabase upgradableDatabase = new UpgradableDatabase( fs, storeVersionCheck,
                    legacyStoreVersionCheck, toFormat );
            for ( RecordFormats fromFormat : recordFormatses )
            {
                File db = new File( dir, fromFormat.storeVersion() + toFormat.storeVersion() );
                try
                {
                    createDb( fromFormat, pageCache, fs, db );
                    if ( !upgradableDatabase.hasCurrentVersion( db ) )
                    {
                        upgradableDatabase.checkUpgradeable( db );
                        data.add( new Object[]{fromFormat, toFormat} );
                    }
                }
                catch ( Exception e )
                {
                    //This means that the combination is not migratable.
                }
                fs.deleteRecursively( db );
            }
        }

        return data;
    }

    private static void addIfNotThere( RecordFormats f, ArrayList<RecordFormats> recordFormatses )
    {
        for ( RecordFormats format : recordFormatses )
        {
            if ( format.storeVersion().equals( f.storeVersion() ) )
            {
                return;
            }
        }
        recordFormatses.add( f );
    }

    @Service.Implementation( RecordFormats.Factory.class )
    public static class Standard23Factory extends RecordFormats.Factory
    {
        public Standard23Factory()
        {
            super( StandardV2_3.STORE_VERSION );
        }

        @Override
        public RecordFormats newInstance()
        {
            return StandardV2_3.RECORD_FORMATS;
        }
    }

    @Service.Implementation( RecordFormats.Factory.class )
    public static class Standard30Factory extends RecordFormats.Factory
    {
        public Standard30Factory()
        {
            super( StandardV3_0.STORE_VERSION );
        }

        @Override
        public RecordFormats newInstance()
        {
            return StandardV3_0.RECORD_FORMATS;
        }
    }

    private static void createDb( RecordFormats recordFormat, PageCache pageCache, FileSystemAbstraction fs,
            File storeDir ) throws IOException
    {
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat.storeVersion() ).newGraphDatabase();
        database.shutdown();
    }

    public StoreMigrationIT( RecordFormats from, RecordFormats to )
    {
        this.from = from;
        this.to = to;
    }

    @Test
    public void shouldMigrate() throws Exception
    {
        File db = testDir.directory( from.toString() + to.toString() );
        FileSystemAbstraction fs = fileSystemRule.get();
        fs.deleteRecursively( db );
        GraphDatabaseService database = getGraphDatabaseService( db, from.storeVersion() );

        database.execute( "CREATE INDEX ON :Person(name)" );
        database.execute( "CREATE INDEX ON :Person(born)" );
        database.execute( "CREATE CONSTRAINT ON (person:Person) ASSERT exists(person.name)" );
        database.execute( CREATE_QUERY );
        int beforeNodes;
        int beforeLabels;
        int beforeKeys;
        int beforeRels;
        int beforeRelTypes;
        int beforeIndexes;
        int beforeConstraints;
        String[] beforeIndexNames;
        try ( Transaction tx = database.beginTx() )
        {
            beforeNodes = database.getAllNodes().stream().mapToInt( ( n ) -> 1 ).sum();
            beforeLabels = database.getAllLabels().stream().mapToInt( ( n ) -> 1 ).sum();
            beforeKeys = database.getAllPropertyKeys().stream().mapToInt( ( n ) -> 1 ).sum();
            beforeRels = database.getAllRelationships().stream().mapToInt( ( n ) -> 1 ).sum();
            beforeRelTypes = database.getAllRelationshipTypes().stream().mapToInt( ( n ) -> 1 ).sum();
            Stream<IndexDefinition> indexDefinitionStream = StreamSupport
                    .stream( database.schema().getIndexes().spliterator(), false );
            beforeIndexes = indexDefinitionStream.mapToInt( ( n ) -> 1 ).sum();
            Stream<ConstraintDefinition> constraintStream = StreamSupport
                    .stream( database.schema().getConstraints().spliterator(), false );
            beforeConstraints = constraintStream.mapToInt( ( n ) -> 1 ).sum();
        }
        database.shutdown();

        database = getGraphDatabaseService( db, to.storeVersion() );
        int afterNodes;
        int afterLabels;
        int afterKeys;
        int afterRels;
        int afterRelTypes;
        int afterIndexes;
        int afterConstraints;
        try ( Transaction tx = database.beginTx() )
        {
            afterNodes = database.getAllNodes().stream().mapToInt( ( n ) -> 1 ).sum();
            afterLabels = database.getAllLabels().stream().mapToInt( ( n ) -> 1 ).sum();
            afterKeys = database.getAllPropertyKeys().stream().mapToInt( ( n ) -> 1 ).sum();
            afterRels = database.getAllRelationships().stream().mapToInt( ( n ) -> 1 ).sum();
            afterRelTypes = database.getAllRelationshipTypes().stream().mapToInt( ( n ) -> 1 ).sum();
            Stream<IndexDefinition> indexDefinitionStream = StreamSupport
                    .stream( database.schema().getIndexes().spliterator(), false );
            afterIndexes = indexDefinitionStream.mapToInt( ( n ) -> 1 ).sum();
            Stream<ConstraintDefinition> constraintStream = StreamSupport
                    .stream( database.schema().getConstraints().spliterator(), false );
            afterConstraints = constraintStream.mapToInt( ( n ) -> 1 ).sum();
        }
        database.shutdown();

        assertEquals( beforeNodes, afterNodes ); //171
        assertEquals( beforeLabels, afterLabels ); //2
        assertEquals( beforeKeys, afterKeys ); //8
        assertEquals( beforeRels, afterRels ); //253
        assertEquals( beforeRelTypes, afterRelTypes ); //6
        assertEquals( beforeIndexes, afterIndexes ); //2
        assertEquals( beforeConstraints, afterConstraints ); //1
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( );
        ConsistencyCheckService.Result result = runConsistencyChecker( db, fs, consistencyCheckService,
                to.storeVersion() );
        if( !result.isSuccessful() )
        {
            fail( "Database is inconsistent after migration." );
        }
    }

    //This method is overridden by a blockdevice test.
    protected ConsistencyCheckService.Result runConsistencyChecker( File db, FileSystemAbstraction fs,
            ConsistencyCheckService consistencyCheckService, String storeVersion )
            throws ConsistencyCheckIncompleteException, IOException
    {
        Config config = Config.defaults();
        config.augment( MapUtil.stringMap( GraphDatabaseSettings.record_format.name(), storeVersion ) );
        return consistencyCheckService
                    .runFullConsistencyCheck( db, config, ProgressMonitorFactory.NONE,
                            NullLogProvider.getInstance(), fs, false );
    }

    //This method is overridden by a blockdevice test.
    protected GraphDatabaseService getGraphDatabaseService( File db, String storeVersion )
    {
        return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( db )
                .setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, storeVersion ).newGraphDatabase();
    }
}
