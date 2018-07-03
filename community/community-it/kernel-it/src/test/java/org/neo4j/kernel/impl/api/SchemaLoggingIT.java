/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.index.IndexPopulationJob;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.neo4j.graphdb.Label.label;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class SchemaLoggingIT
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Rule
    public final ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule( logProvider );

    @Test
    public void shouldLogUserReadableLabelAndPropertyNames() throws Exception
    {
        //noinspection deprecation
        GraphDatabaseAPI db = dbRule.getGraphDatabaseAPI();

        String labelName = "User";
        String property = "name";

        // when
        createIndex( db, labelName, property );

        // then
        LogMatcherBuilder match = inLog( IndexPopulationJob.class );
        logProvider.assertAtLeastOnce( match.info( "Index population started: [%s]", ":User(name) [provider: {key=lucene+native, version=2.0}]" ) );

        assertEventually( (ThrowingSupplier<Object,Exception>) () -> null, new LogMessageMatcher( match ), 1, TimeUnit.MINUTES );
    }

    private void createIndex( GraphDatabaseAPI db, String labelName, String property )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label( labelName ) ).on( property ).create();
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private class LogMessageMatcher extends BaseMatcher<Object>
    {
        private static final String CREATION_FINISHED = "Index creation finished. Index [%s] is %s.";
        private final LogMatcherBuilder match;

        LogMessageMatcher( LogMatcherBuilder match )
        {
            this.match = match;
        }

        @Override
        public boolean matches( Object item )
        {
            return logProvider.containsMatchingLogCall(
                    match.info( CREATION_FINISHED, ":User(name) [provider: {key=lucene+native, version=2.0}]", "ONLINE" ) );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( " expected log message: '" ).appendText( CREATION_FINISHED )
                    .appendText( "', but not found. Messages was: '" ).appendText( logProvider.serialize() ).appendText( "." );
        }
    }
}
