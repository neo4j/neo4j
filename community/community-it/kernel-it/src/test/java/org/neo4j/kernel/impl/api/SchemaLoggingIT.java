/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.api.index.IndexPopulationJob;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.Matchers.containsString;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
class SchemaLoggingIT
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private GraphDatabaseAPI db;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setInternalLogProvider( logProvider );
    }

    @Test
    void shouldLogUserReadableLabelAndPropertyNames() throws Exception
    {
        String labelName = "User";
        String property = "name";

        // when
        createIndex( db, labelName, property );

        // then
        LogMatcherBuilder match = inLog( IndexPopulationJob.class );
        IndexProviderMap indexProviderMap = db.getDependencyResolver().resolveDependency( IndexProviderMap.class );
        IndexProvider defaultProvider = indexProviderMap.getDefaultProvider();
        IndexProviderDescriptor providerDescriptor = defaultProvider.getProviderDescriptor();
        logProvider.assertAtLeastOnce( match.info( containsString( "Index population started: [%s]" ),
                ":User(name) [provider: {key=" + providerDescriptor.getKey() + ", version=" + providerDescriptor.getVersion() + "}]" ) );

        assertEventually( () -> null, new LogMessageMatcher( match, providerDescriptor ), 1, TimeUnit.MINUTES );
    }

    private static void createIndex( GraphDatabaseAPI db, String labelName, String property )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( labelName ) ).on( property ).create();
            tx.commit();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private class LogMessageMatcher extends BaseMatcher<Object>
    {
        private static final String CREATION_FINISHED = "Index creation finished. Index [%s] is %s.";
        private final LogMatcherBuilder match;
        private final IndexProviderDescriptor descriptor;

        LogMessageMatcher( LogMatcherBuilder match, IndexProviderDescriptor descriptor )
        {
            this.match = match;
            this.descriptor = descriptor;
        }

        @Override
        public boolean matches( Object item )
        {
            return logProvider.containsMatchingLogCall( match.info( containsString( CREATION_FINISHED ),
                    ":User(name) [provider: {key=" + descriptor.getKey() + ", version=" + descriptor.getVersion() + "}]", "ONLINE" ) );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( " expected log message containing: '" ).appendText( CREATION_FINISHED )
                    .appendText( "', but not found. Messages was: '" ).appendText( logProvider.serialize() ).appendText( "." );
        }
    }
}
