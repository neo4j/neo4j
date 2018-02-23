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
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

import org.neo4j.cypher.internal.CommunityCompatibilityFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@ExtendWith( ImpermanentDatabaseExtension.class )
public class ExecutionEngineTest
{
    private static final Map<String,Object> NO_PARAMS = Collections.emptyMap();

    @Resource
    public ImpermanentDatabaseRule database;

    @Test
    public void shouldConvertListsAndMapsWhenPassingFromScalaToJava() throws Exception
    {
        GraphDatabaseQueryService graph = new GraphDatabaseCypherService( this.database.getGraphDatabaseAPI() );
        Monitors monitors = graph.getDependencyResolver().resolveDependency( Monitors.class );

        NullLogProvider nullLogProvider = NullLogProvider.getInstance();
        CommunityCompatibilityFactory compatibilityFactory =
                new CommunityCompatibilityFactory( graph, monitors, nullLogProvider );
        ExecutionEngine executionEngine = new ExecutionEngine( graph, nullLogProvider, compatibilityFactory );

        Result result;
        try ( InternalTransaction tx = graph
                .beginTransaction( KernelTransaction.Type.implicit, LoginContext.AUTH_DISABLED ) )
        {
            String query = "RETURN { key : 'Value' , collectionKey: [{ inner: 'Map1' }, { inner: 'Map2' }]}";
            TransactionalContext tc = createTransactionContext( graph, tx, query );
            result = executionEngine.executeQuery( query, NO_PARAMS, tc );

            verifyResult( result );

            result.close();
            tx.success();
        }
    }

    private void verifyResult( Result result )
    {
        Map firstRowValue = (Map) result.next().values().iterator().next();
        assertThat( firstRowValue.get( "key" ), is( "Value" ) );
        List theList = (List) firstRowValue.get( "collectionKey" );
        assertThat( ((Map) theList.get( 0 )).get( "inner" ), is( "Map1" ) );
        assertThat( ((Map) theList.get( 1 )).get( "inner" ), is( "Map2" ) );
    }

    private TransactionalContext createTransactionContext( GraphDatabaseQueryService graph, InternalTransaction tx,
            String query )
    {
        PropertyContainerLocker locker = new PropertyContainerLocker();
        TransactionalContextFactory contextFactory = Neo4jTransactionalContextFactory.create( graph, locker );
        return contextFactory.newContext( ClientConnectionInfo.EMBEDDED_CONNECTION, tx, query, EMPTY_MAP );
    }
}
