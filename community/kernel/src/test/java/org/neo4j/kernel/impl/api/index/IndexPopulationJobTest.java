/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ThreadToStatementContextBridge;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.test.ImpermanentGraphDatabase;

public class IndexPopulationJobTest
{
    @Test
    public void shouldPopulateIndexWithOneNode() throws Exception
    {
        // GIVEN
        String value = "Taylor";
        long nodeId = createNode( map( name, value ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name );

        // WHEN
        job.run();

        // THEN
        verify( manipulator ).add( 0, nodeId, value );
        verify( manipulator ).done();
        verifyNoMoreInteractions( manipulator );
    }

    @Test
    public void shouldPopulateIndexWithASmallDataset() throws Exception
    {
        // GIVEN
        String value = "Mattias";
        long node1 = createNode( map( name, value ), FIRST );
        createNode( map( name, value ), SECOND );
        createNode( map( age, 31 ), FIRST );
        long node4 = createNode( map( age, 35, name, value ), FIRST );
        IndexPopulationJob job = newIndexPopulationJob( FIRST, name );

        // WHEN
        job.run();

        // THEN
        verify( manipulator ).add( anyInt(), eq( node1 ), eq( value ) );
        verify( manipulator ).add( anyInt(), eq( node4 ), eq( value ) );
        verify( manipulator ).done();
        verifyNoMoreInteractions( manipulator );
    }
    
    private ImpermanentGraphDatabase db;
    private final Label FIRST = DynamicLabel.label( "FIRST" ), SECOND = DynamicLabel.label( "SECOND" );
    private final String name = "name", age = "age";
    private ThreadToStatementContextBridge ctxProvider;
    private StatementContext context;
    private IndexPopulator manipulator;
    
    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase();
        ctxProvider = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        context = ctxProvider.getCtxForReading();
        manipulator = mock( IndexPopulator.class );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }

    private IndexPopulationJob newIndexPopulationJob( Label label, String propertyKey )
            throws LabelNotFoundKernelException, PropertyKeyNotFoundException
    {
        return new IndexPopulationJob(
                context.getLabelId( FIRST.name() ),
                context.getPropertyKeyId( name ),
                manipulator, db.getXaDataSourceManager().getNeoStoreDataSource().getNeoStore() );
    }

    private long createNode( Map<String, Object> properties, Label... labels )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String, Object> property : properties.entrySet() )
                node.setProperty( property.getKey(), property.getValue() );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }
}
