/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.javacompat.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.cypher.internal.DocsExecutionEngine;
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.InternalExecutionResult;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QuerySource;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DocsExecutionEngineTest
{
    private static GraphDatabaseCypherService database;
    private static DocsExecutionEngine engine;
    private static Neo4jTransactionalContextFactory contextFactory;

    @Before
    public void setup()
    {
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        database = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().setFileSystem( fs ).newImpermanentDatabase());
        engine = new DocsExecutionEngine( database, NullLogProvider.getInstance() );
        contextFactory = new Neo4jTransactionalContextFactory( database, new PropertyContainerLocker() );
    }

    @After
    public void teardown()
    {
        database.getGraphDatabaseService().shutdown();
    }

    @Test
    public void actually_works_in_rewindable_fashion()
    {
        String query = "CREATE (n:Person {name:'Adam'}) RETURN n";
        InternalExecutionResult result = engine.internalProfile( query, Collections.emptyMap(),
                createTransactionalContext( query ) );
        String dump = result.dumpToString();
        assertThat( dump, containsString( "1 row" ) );
        assertThat( result.javaIterator().hasNext(), equalTo( true ) );
    }

    @Test
    public void should_work_in_rewindable_fashion()
    {
        String query = "RETURN 'foo'";
        InternalExecutionResult result = engine.internalProfile( query, Collections.emptyMap(),
                createTransactionalContext( query ) );
        String dump = result.dumpToString();
        assertThat( dump, containsString( "1 row" ) );
        assertThat( result.javaIterator().hasNext(), equalTo( true ) );
    }

    public static Neo4jTransactionalContext createTransactionalContext( String query )
    {
        InternalTransaction transaction = database.beginTransaction( KernelTransaction.Type.implicit, AccessMode.Static.FULL );
        return contextFactory.newContext(
                QuerySource.UNKNOWN, transaction, query, Collections.emptyMap() );
    }
}
