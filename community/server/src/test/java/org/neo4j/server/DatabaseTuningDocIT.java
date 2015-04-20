/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.BufferingLogging;
import org.neo4j.test.server.ExclusiveServerTestBase;

public class DatabaseTuningDocIT extends ExclusiveServerTestBase
{
    @Ignore("Relies on internal config, which is bad")
    @Test
    public void shouldLoadAKnownGoodPropertyFile() throws IOException
    {
        CommunityNeoServer server = CommunityServerBuilder.server()
                .usingDatabaseDir( folder.cleanDirectory( name.getMethodName() ).getAbsolutePath() )
                .withDefaultDatabaseTuning()
                .build();
        server.start();
        Map<Object, Object> params = null; // TODO This relies on internal stuff,
        // which is no good: server.getDatabase().graph.getConfig().getParams();


        assertTrue( propertyAndValuePresentIn( "neostore.nodestore.db.mapped_memory", "25M", params ) );
        assertTrue( propertyAndValuePresentIn( "neostore.relationshipstore.db.mapped_memory", "50M", params ) );
        assertTrue( propertyAndValuePresentIn( "neostore.propertystore.db.mapped_memory", "90M", params ) );
        assertTrue( propertyAndValuePresentIn( "neostore.propertystore.db.strings.mapped_memory", "130M", params ) );
        assertTrue( propertyAndValuePresentIn( "neostore.propertystore.db.arrays.mapped_memory", "130M", params ) );

        server.stop();
    }

    private boolean propertyAndValuePresentIn( String name, String value, Map<Object, Object> params )
    {
        Object paramValue = params.get( name );
        return paramValue != null && paramValue.toString().equals( value );
    }

    @Test
    public void shouldLogWarningAndContinueIfTuningFilePropertyDoesNotResolve() throws IOException
    {
        Logging logging = new BufferingLogging();
        NeoServer server = CommunityServerBuilder.server()
                .usingDatabaseDir( folder.cleanDirectory( name.getMethodName() ).getAbsolutePath() )
                .withNonResolvableTuningFile()
                .withLogging( logging )
                .build();
        server.start();

        String logDump = logging.toString();
        assertThat( logDump,
                containsString( String.format( "The specified file for database performance tuning properties [" ) ) );
        assertThat( logDump, containsString( String.format( "] does not exist." ) ) );

        server.stop();
    }
}
