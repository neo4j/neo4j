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
package org.neo4j.ext.udc.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.StartupStatistics;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class DefaultUdcInformationCollectorTest
{
    private final DefaultUdcInformationCollector collector = new DefaultUdcInformationCollector( new Config(), null,
            new StubKernelData() );

    @Test
    public void shouldIncludeTheMacAddress()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.MAC ) );
    }

    @Test
    public void shouldIncludeTheNumberOfProcessors()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.NUM_PROCESSORS ) );
    }

    @Test
    public void shouldIncludeTotalMemorySize()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.TOTAL_MEMORY ) );
    }

    @Test
    public void shouldIncludeHeapSize()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.HEAP_SIZE ) );
    }

    @Test
    public void shouldIncludeNodeIdsInUse()
    {
        assertEquals( "100", collector.getUdcParams().get( UdcConstants.NODE_IDS_IN_USE ) );
    }

    @Test
    public void shouldIncludeRelationshipIdsInUse()
    {
        assertEquals( "200", collector.getUdcParams().get( UdcConstants.RELATIONSHIP_IDS_IN_USE ) );
    }

    @Test
    public void shouldIncludePropertyIdsInUse()
    {
        assertEquals( "400", collector.getUdcParams().get( UdcConstants.PROPERTY_IDS_IN_USE ) );
    }

    @Test
    public void shouldIncludeLabelIdsInUse()
    {
        assertEquals( "300", collector.getUdcParams().get( UdcConstants.LABEL_IDS_IN_USE ) );
    }

    @SuppressWarnings("deprecation")
    private static class StubKernelData extends KernelData
    {
        public StubKernelData()
        {
            super( new Config() );
        }

        @Override
        public Version version()
        {
            return new Version( "foo", "bar" )
            {

            };
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return new StubDatabase();
        }
    }

    @SuppressWarnings("deprecation")
    private static class StubDatabase implements GraphDatabaseAPI
    {
        private final IdGeneratorFactory idGeneratorFactory = new StubIdGeneratorFactory();

        @Override
        public DependencyResolver getDependencyResolver()
        {
            return new StubDependencyResolver( idGeneratorFactory );
        }

        @Override
        public StoreId storeId()
        {
            return null;
        }

        @Override
        public String getStoreDir()
        {
            return null;
        }

        @Override
        public Node createNode()
        {
            return null;
        }

        @Override
        public Node createNode( Label... labels )
        {
            return null;
        }

        @Override
        public Node getNodeById( long id )
        {
            return null;
        }

        @Override
        public Relationship getRelationshipById( long id )
        {
            return null;
        }

        @Override
        public Iterable<Node> getAllNodes()
        {
            return null;
        }

        @Override
        public ResourceIterator<Node> findNodes( Label label, String key, Object value )
        {
            return null;
        }

        @Override
        public Node findNode( Label label, String key, Object value )
        {
            return null;
        }

        @Override
        public ResourceIterator<Node> findNodes( Label label )
        {
            return null;
        }

        @Override
        public ResourceIterable<Node> findNodesByLabelAndProperty( Label label, String key, Object value )
        {
            return null;
        }

        @Override
        public Iterable<RelationshipType> getRelationshipTypes()
        {
            return null;
        }

        @Override
        public boolean isAvailable( long timeout )
        {
            return false;
        }

        @Override
        public void shutdown()
        {

        }

        @Override
        public Transaction beginTx()
        {
            return null;
        }

        @Override
        public Result execute( String query )
        {
            return execute( query, Collections.<String, Object>emptyMap() );
        }

        @Override
        public Result execute( String query, Map<String, Object> parameters )
        {
            return null;
        }

        @Override
        public <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T>
                                                                                       handler )
        {
            return null;
        }

        @Override
        public <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T>
                                                                                         handler )
        {
            return null;
        }

        @Override
        public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
        {
            return null;
        }

        @Override
        public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
        {
            return null;
        }

        @Override
        public Schema schema()
        {
            return null;
        }

        @Override
        public IndexManager index()
        {
            return null;
        }

        @Override
        public TraversalDescription traversalDescription()
        {
            return null;
        }

        @Override
        public BidirectionalTraversalDescription bidirectionalTraversalDescription()
        {
            return null;
        }
    }

    private static class StubDependencyResolver implements DependencyResolver
    {
        private final IdGeneratorFactory idGeneratorFactory;

        public StubDependencyResolver( IdGeneratorFactory idGeneratorFactory )
        {
            this.idGeneratorFactory = idGeneratorFactory;
        }

        @Override
        public <T> T resolveDependency( Class<T> type ) throws IllegalArgumentException
        {
            if ( IdGeneratorFactory.class.isAssignableFrom( type ) )
            {
                return (T) idGeneratorFactory;
            }
            else if ( StartupStatistics.class.isAssignableFrom( type ) )
            {
                return (T) mock( StartupStatistics.class );
            }

            throw new IllegalArgumentException( type.getName() );
        }

        @Override
        public <T> T resolveDependency( Class<T> type, SelectionStrategy selector ) throws IllegalArgumentException
        {
            return null;
        }

    }

    private static class StubIdGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType, Long> idsInUse = new HashMap<>();

        {
            idsInUse.put( IdType.NODE, 100l );
            idsInUse.put( IdType.RELATIONSHIP, 200l );
            idsInUse.put( IdType.LABEL_TOKEN, 300l );
            idsInUse.put( IdType.PROPERTY, 400l );
        }

        @Override
        public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
        {
            return get( idType );
        }

        @Override
        public void create( FileSystemAbstraction fs, File fileName, long highId )
        {   // Ignore
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return new StubIdGenerator( idsInUse.get( idType ) );
        }
    }

    private static class StubIdGenerator implements IdGenerator
    {
        private final long numberOfIdsInUse;

        public StubIdGenerator( long numberOfIdsInUse )
        {
            this.numberOfIdsInUse = numberOfIdsInUse;
        }

        @Override
        public long nextId()
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public void setHighId( long id )
        {
            throw new UnsupportedOperationException( "Please implement" );
        }

        @Override
        public long getHighId()
        {
            return 0;
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return 0;
        }

        @Override
        public void freeId( long id )
        {   // Ignore
        }

        @Override
        public void close()
        {   // Ignore
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return numberOfIdsInUse;
        }

        @Override
        public long getDefragCount()
        {
            return 0;
        }

        @Override
        public void delete()
        {   // Ignore
        }
    }
}
