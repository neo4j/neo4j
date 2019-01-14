/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.neo4j.server.configuration.ServerSettings;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.neo4j.driver.v1.Values.parameters;

@RunWith( Parameterized.class )
public class BoltDriverLargePropertiesIT
{
    @ClassRule
    public static final Neo4jRule db = new Neo4jRule()
            .withConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
            .withConfig( ServerSettings.script_enabled, Settings.TRUE );

    private static Driver driver;

    @Parameter
    public int size;

    @BeforeClass
    public static void setUp() throws Exception
    {
        driver = GraphDatabase.driver( db.boltURI() );
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if ( driver != null )
        {
            driver.close();
        }
    }

    @Parameters( name = "{0}" )
    public static List<Object> arraySizes()
    {
        return Arrays.asList( 1, 2, 3, 10, 999, 4_295, 10_001, 55_155, 100_000 );
    }

    @Test
    public void shouldSendAndReceiveString()
    {
        String originalValue = RandomStringUtils.randomAlphanumeric( size );
        Object receivedValue = sendAndReceive( originalValue );
        assertEquals( originalValue, receivedValue );
    }

    @Test
    @SuppressWarnings( "unchecked" )
    public void shouldSendAndReceiveLongArray()
    {
        List<Long> originalLongs = ThreadLocalRandom.current().longs( size ).boxed().collect( toList() );
        List<Long> receivedLongs = (List<Long>) sendAndReceive( originalLongs );
        assertEquals( originalLongs, receivedLongs );
    }

    private static Object sendAndReceive( Object value )
    {
        try ( Session session = driver.session() )
        {
            StatementResult result = session.run( "RETURN $value", parameters( "value", value ) );
            Record record = result.single();
            return record.get( 0 ).asObject();
        }
    }
}
