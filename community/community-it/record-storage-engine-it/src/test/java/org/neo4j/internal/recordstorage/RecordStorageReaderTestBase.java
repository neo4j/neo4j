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
package org.neo4j.internal.recordstorage;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.api.ClockContext;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RecordStorageEngineRule;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

/**
 * Base class for disk layer tests, which test read-access to committed data.
 */
public abstract class RecordStorageReaderTestBase
{
    private static final ResourceLocker IGNORE_LOCKING = ( tracer, resourceType, resourceIds ) -> {};

    private final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();
    private final RecordStorageEngineRule storageEngineRule = new RecordStorageEngineRule();

    @Rule
    public final RuleChain rules = RuleChain.outerRule( storage ).around( storageEngineRule );

    protected final Label label1 = label( "FirstLabel" );
    protected final Label label2 = label( "SecondLabel" );
    protected final RelationshipType relType1 = RelationshipType.withName( "type1" );
    protected final RelationshipType relType2 = RelationshipType.withName( "type2" );
    protected final String propertyKey = "name";
    protected final String otherPropertyKey = "age";
    private final AtomicLong nextTxId = new AtomicLong( TransactionIdStore.BASE_TX_ID );
    private TokenHolders tokenHolders;
    protected KernelStatement state;
    protected RecordStorageReader storageReader;
    protected RecordStorageEngine storageEngine;
    private RecordStorageReader commitReader;
    private CommandCreationContext commitContext;

    @SuppressWarnings( "deprecation" )
    @Before
    public void before()
    {
        this.tokenHolders = new TokenHolders(
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_PROPERTY_KEY ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_LABEL ),
                new DelegatingTokenHolder( new SimpleTokenCreator(), TokenHolder.TYPE_RELATIONSHIP_TYPE ) );
        RecordStorageEngineRule.Builder builder =
                storageEngineRule.getWith( storage.fileSystem(), storage.pageCache(), storage.directory().databaseLayout() ).tokenHolders( tokenHolders );
        builder = modify( builder );
        this.storageEngine = builder.build();
        this.storageReader = storageEngine.newReader();
        this.state = new KernelStatement( null, null, LockTracer.NONE, null, new ClockContext(), EmptyVersionContextSupplier.EMPTY );
        this.commitReader = storageEngine.newReader();
        this.commitContext = storageEngine.newCommandCreationContext();
    }

    RecordStorageEngineRule.Builder modify( RecordStorageEngineRule.Builder builder )
    {
        return builder;
    }

    protected long createNode( Map<String,Object> properties, Label... labels ) throws Exception
    {
        TxState txState = new TxState();
        long nodeId = commitContext.reserveNode();
        txState.nodeDoCreate( nodeId );
        for ( Label label : labels )
        {
            txState.nodeDoAddLabel( getOrCreateLabelId( label ), nodeId );
        }
        for ( Map.Entry<String,Object> property : properties.entrySet() )
        {
            txState.nodeDoAddProperty( nodeId, getOrCreatePropertyKeyId( property.getKey() ), Values.of( property.getValue() ) );
        }
        apply( txState );
        return nodeId;
    }

    protected void deleteNode( long nodeId ) throws Exception
    {
        TxState txState = new TxState();
        txState.nodeDoDelete( nodeId );
        apply( txState );
    }

    protected long createRelationship( long sourceNode, long targetNode, RelationshipType relationshipType ) throws Exception
    {
        TxState txState = new TxState();
        long relationshipId = commitContext.reserveRelationship();
        txState.relationshipDoCreate( relationshipId, getOrCreateRelationshipTypeId( relationshipType ), sourceNode, targetNode );
        apply( txState );
        return relationshipId;
    }

    protected void deleteRelationship( long relationshipId ) throws Exception
    {
        TxState txState = new TxState();
        try ( RecordRelationshipScanCursor cursor = commitReader.allocateRelationshipScanCursor() )
        {
            cursor.single( relationshipId );
            assertTrue( cursor.next() );
            txState.relationshipDoDelete( relationshipId, cursor.type(), cursor.getFirstNode(), cursor.getSecondNode() );
        }
        apply( txState );
    }

    protected void createUniquenessConstraint( Label label, String propertyKey ) throws Exception
    {
        createIndex( label, propertyKey );
        TxState txState = new TxState();
        txState.constraintDoAdd( ConstraintDescriptorFactory.uniqueForLabel( getOrCreateLabelId( label ), getOrCreatePropertyKeyId( propertyKey ) ) );
        apply( txState );
    }

    protected void createNodeKeyConstraint( Label label, String propertyKey ) throws Exception
    {
        createIndex( label, propertyKey );
        TxState txState = new TxState();
        txState.constraintDoAdd( ConstraintDescriptorFactory.nodeKeyForLabel( getOrCreateLabelId( label ), getOrCreatePropertyKeyId( propertyKey ) ) );
        apply( txState );
    }

    private void createIndex( Label label, String propertyKey ) throws Exception
    {
        TxState txState = new TxState();
        txState.indexDoAdd( IndexDescriptorFactory.uniqueForSchema(
                SchemaDescriptorFactory.forLabel( getOrCreateLabelId( label ), getOrCreatePropertyKeyId( propertyKey ) ) ) );
        apply( txState );
    }

    protected void createNodePropertyExistenceConstraint( Label label, String propertyKey ) throws Exception
    {
        TxState txState = new TxState();
        txState.constraintDoAdd( ConstraintDescriptorFactory.existsForLabel( getOrCreateLabelId( label ), getOrCreatePropertyKeyId( propertyKey ) ) );
        apply( txState );
    }

    protected void createRelPropertyExistenceConstraint( RelationshipType relationshipType, String propertyKey ) throws Exception
    {
        TxState txState = new TxState();
        txState.constraintDoAdd( ConstraintDescriptorFactory.existsForRelType( getOrCreateRelationshipTypeId( relationshipType ),
                getOrCreatePropertyKeyId( propertyKey ) ) );
        apply( txState );
    }

    private int getOrCreatePropertyKeyId( String propertyKey ) throws KernelException
    {
        return tokenHolders.propertyKeyTokens().getOrCreateId( propertyKey );
    }

    private int getOrCreateLabelId( Label label ) throws KernelException
    {
        return tokenHolders.labelTokens().getOrCreateId( label.name() );
    }

    private int getOrCreateRelationshipTypeId( RelationshipType relationshipType ) throws KernelException
    {
        return tokenHolders.relationshipTypeTokens().getOrCreateId( relationshipType.name() );
    }

    private void apply( TxState txState ) throws Exception
    {
        List<StorageCommand> commands = new ArrayList<>();
        long txId = nextTxId.incrementAndGet();
        storageEngine.createCommands( commands, txState, commitReader, commitContext, IGNORE_LOCKING, txId, state -> state );
        storageEngine.apply( new GroupOfCommands( txId, commands.toArray( new StorageCommand[0] ) ), TransactionApplicationMode.EXTERNAL );
    }

    protected int labelId( Label label )
    {
        return tokenHolders.labelTokens().getIdByName( label.name() );
    }

    protected int relationshipTypeId( RelationshipType type )
    {
        return tokenHolders.relationshipTypeTokens().getIdByName( type.name() );
    }

    protected String relationshipType( int id ) throws KernelException
    {
        try
        {
            return tokenHolders.relationshipTypeTokens().getTokenById( id ).name();
        }
        catch ( TokenNotFoundException e )
        {
            throw new RelationshipTypeIdNotFoundKernelException( id, e );
        }
    }

    protected int propertyKeyId( String propertyKey )
    {
        return tokenHolders.propertyKeyTokens().getIdByName( propertyKey );
    }
}
