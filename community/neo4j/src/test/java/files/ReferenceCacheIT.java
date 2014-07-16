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
package files;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.cache.SoftCacheProvider;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

@Ignore("This is not a real test but something useful to reproduce a memory leak in ReferenceCache")
public class ReferenceCacheIT
{
    @Rule
    public TargetDirectory.TestDirectory directory =
            TargetDirectory.testDirForTest( ReferenceCacheIT.class );
    private GraphDatabaseAPI db;
    private ExecutionEngine engine;

    @Before
    public void setUp()
    {
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory.absolutePath() )
                .setConfig( GraphDatabaseSettings.cache_type.name(), SoftCacheProvider.NAME )
                .newGraphDatabase();
        engine = new ExecutionEngine( db );
    }

    @Test
    public void test()
    {
        // Given indexes
        int nrPeople = 1000;
        int nrAddresses = 100000;
        int nrPhones = 50000;

        Map<String, String>[] people = new HashMap[nrPeople];
        Map<String, String>[] addresses = new HashMap[nrAddresses];
        String[] phones = new String[nrAddresses];

        for ( int i = 0; i < nrAddresses; i++ )
        {
            addresses[i] = new HashMap<>();
            addresses[i].put( "street", "STREET" + i );
            addresses[i].put( "city", "CITY" + i );
            addresses[i].put( "zipcode", "ZIPCODE" + i );
            addresses[i].put( "state", "STATE" + i );
            addresses[i].put( "uuid", "AUUID" + i );
        }

        for ( int i = 0; i < nrPhones; i++ )
        {
            phones[i] = "" + i;
        }

        try ( Transaction tx = db.beginTx() )
        {
            engine.execute( "CREATE INDEX ON :Person(first_name)" );
            engine.execute( "CREATE INDEX ON :Person(last_name)" );
            engine.execute( "CREATE INDEX ON :Person(uuid)" );
            engine.execute( "CREATE INDEX ON :Address(uuid)" );
            engine.execute( "CREATE INDEX ON :Phone(number)" );
            tx.success();
        }

        Random random = new Random( System.currentTimeMillis() );

        for ( int i = 0; i < Integer.MAX_VALUE; i++ )
        {
            System.out.println( "====== Start " + i + " ======" );

            try ( Transaction tx = db.beginTx() )
            {
                updatePeople( i, people, nrPeople );
                for ( int j = 0; j < nrPeople; j++ )
                {
                    Map<String, String> address1 = addresses[random.nextInt( nrAddresses )];
                    Map<String, String> address2 = addresses[random.nextInt( nrAddresses )];
                    String phone = phones[random.nextInt( nrPhones )];

                    engine.execute( "CREATE (n:Person {props})",
                            Collections.<String, Object>singletonMap( "props", people[j] ) );
                    engine.execute( "MERGE (n: Address {uuid: {props}.uuid}) ON CREATE SET n = {props}",
                            Collections.<String, Object>singletonMap( "props", address1 ) );
                    engine.execute( "MERGE (n: Address {uuid: {props}.uuid}) ON CREATE SET n = {props}",
                            Collections.<String, Object>singletonMap( "props", address2 ) );
                    engine.execute( "MERGE (n: Phone {number: {number}}) ",
                            Collections.<String, Object>singletonMap( "number", phone ) );

                    Map<String, Object> params = new HashMap<>();
                    params.put( "person_uuid", people[j].get( "uuid" ) );
                    params.put( "addr1_uuid", address1.get( "uuid" ) );
                    params.put( "addr2_uuid", address2.get( "uuid" ) );
                    params.put( "phone_number", phone );

                    engine.execute(
                            "MATCH (person: Person {uuid: {person_uuid}})," +
                                    "      (addr1: Address {uuid: {addr1_uuid}})," +
                                    "      (addr2: Address {uuid: {addr2_uuid}})," +
                                    "      (phone: Phone {number: {phone_number}})" +
                                    "CREATE (person)-[: ADDRESS]->(addr1)," +
                                    "       (person)-[: ADDRESS]->(addr2)," +
                                    "       (person)-[: PHONE]->(phone) ",
                            params
                    );
                }
                tx.success();
            }
            System.out.println( "====== End " + i + " ======" );
        }

        db.shutdown();
    }

    private static void updatePeople( int it, Map<String, String>[] people, int nrPeople )
    {
        for ( int i = 0; i < nrPeople; i++ )
        {
            people[i] = new HashMap<>();
            people[i].put( "first_name", "NAME" + i + it );
            people[i].put( "last_name", "SURNAME" + i + it );
            people[i].put( "email", "EMAIL$i$it" + i + it );
            people[i].put( "text", "TEST" + i + it );
            people[i].put( "uuid", "PUUID" + i + it );
        }
    }
}
