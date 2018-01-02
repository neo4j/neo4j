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
package org.neo4j.kernel.impl.api.store;

import org.junit.After;
import org.junit.Before;

import java.util.Map;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.graphdb.DynamicLabel.label;

/**
 * Base class for disk layer tests, which test read-access to committed data.
 */
public class DiskLayerTest
{
    @SuppressWarnings( "deprecation" )
    protected GraphDatabaseAPI db;
    protected final Label label1 = label( "FirstLabel" );
    protected final Label label2 = label( "SecondLabel" );
    protected final RelationshipType relType1 = DynamicRelationshipType.withName( "type1" );
    protected final RelationshipType relType2 = DynamicRelationshipType.withName( "type2" );
    protected final String propertyKey = "name";
    protected final String otherPropertyKey = "age";
    protected KernelStatement state;
    protected DiskLayer disk;

    @SuppressWarnings( "deprecation" )
    @Before
    public void before()
    {
        db = (GraphDatabaseAPI) createGraphDatabase();
        DependencyResolver resolver = db.getDependencyResolver();
        IndexingService indexingService = resolver.resolveDependency( IndexingService.class );
        final NeoStores neoStores = resolver.resolveDependency( NeoStoresSupplier.class ).get();
        this.disk = new DiskLayer(
                resolver.resolveDependency( PropertyKeyTokenHolder.class ),
                resolver.resolveDependency( LabelTokenHolder.class ),
                resolver.resolveDependency( RelationshipTypeTokenHolder.class ),
                new SchemaStorage( neoStores.getSchemaStore() ),
                neoStores,
                indexingService, new Factory<StoreStatement>()
                {
                    @Override
                    public StoreStatement newInstance()
                    {
                        return new StoreStatement( neoStores, LockService.NO_LOCK_SERVICE );
                    }
                } );
        this.state = new KernelStatement( null, new IndexReaderFactory.Caching( indexingService ),
                resolver.resolveDependency( LabelScanStore.class ), null,
                null, null, disk.acquireStatement() );
    }

    protected GraphDatabaseService createGraphDatabase()
    {
        return new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    protected static Node createLabeledNode( GraphDatabaseService db, Map<String,Object> properties, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            tx.success();
            return node;
        }
    }

    protected IndexDescriptor createIndexAndAwaitOnline( Label label, String propertyKey ) throws Exception
    {
        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = db.schema().indexFor( label ).on( propertyKey ).create();
            tx.success();
        }

        try ( Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 10, SECONDS );
            return disk.indexesGetForLabelAndPropertyKey( disk.labelGetForName( label.name() ),
                    disk.propertyKeyGetForName( propertyKey ) );
        }
    }

    protected int labelId( Label label )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return readOps().labelGetForName( label.name() );
        }
    }

    protected int relationshipTypeId( RelationshipType type )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return readOps().relationshipTypeGetForName( type.name() );
        }
    }

    protected int propertyKeyId( String propertyKey )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            return readOps().propertyKeyGetForName( propertyKey );
        }
    }

    protected ReadOperations readOps()
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        Statement statement = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ).get();
        return statement.readOperations();
    }
}
