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
package org.neo4j.kernel.impl.api.store;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.DegreeItem;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.kernel.impl.core.TokenHolder.NO_ID;
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK_SERVICE;
import static org.neo4j.storageengine.api.Direction.BOTH;
import static org.neo4j.storageengine.api.Direction.INCOMING;
import static org.neo4j.storageengine.api.Direction.OUTGOING;

public class StoreSingleNodeCursorTest
{
    private static final int RELATIONSHIPS_COUNT = 20;

    private final RandomRule random = new RandomRule();
    private final DatabaseRule db = new ImpermanentDatabaseRule().startLazily();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( random ).around( db );

    @Before
    public void startDb() throws Exception
    {
        db.setConfig( GraphDatabaseSettings.dense_node_threshold, String.valueOf( RELATIONSHIPS_COUNT ) );
    }

    @Test
    public void relationshipTypesForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception
    {
        testRelationshipTypesForDenseNode(
                this::noNodeChange,
                asSet( TestRelType.IN, TestRelType.OUT, TestRelType.LOOP ) );

        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN ),
                asSet( TestRelType.OUT, TestRelType.LOOP ) );
        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.OUT ),
                asSet( TestRelType.IN, TestRelType.LOOP ) );
        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.LOOP ),
                asSet( TestRelType.IN, TestRelType.OUT ) );

        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN, TestRelType.OUT ),
                asSet( TestRelType.LOOP ) );
        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN, TestRelType.LOOP ),
                asSet( TestRelType.OUT ) );
        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.OUT, TestRelType.LOOP ),
                asSet( TestRelType.IN ) );

        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN, TestRelType.OUT, TestRelType.LOOP ),
                emptySet() );
    }

    @Test
    public void relationshipTypesForDenseNodeWithPartiallyDeletedRelChains() throws Exception
    {
        testRelationshipTypesForDenseNode(
                this::markRandomRelsNotInUse,
                asSet( TestRelType.IN, TestRelType.OUT, TestRelType.LOOP ) );
    }

    @Test
    public void degreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain()
    {
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.OUT );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.LOOP );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.OUT );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.LOOP );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.OUT, TestRelType.LOOP );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.OUT,
                TestRelType.LOOP );
    }

    @Test
    public void degreeByDirectionForDenseNodeWithPartiallyDeletedRelChains()
    {
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( false, false, false );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( true, false, false );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( false, true, false );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( false, false, true );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( true, true, false );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( true, true, true );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( true, false, true );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( true, true, true );
    }

    @Test
    public void degreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception
    {
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.OUT );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.LOOP );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN,
                TestRelType.OUT );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.OUT,
                TestRelType.LOOP );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN,
                TestRelType.LOOP );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.OUT,
                TestRelType.LOOP );
    }

    @Test
    public void degreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains() throws Exception
    {
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( false, false, false );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( true, false, false );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( false, true, false );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( false, false, true );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( true, true, false );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( true, true, true );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( true, false, true );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( true, true, true );
    }

    @Test
    public void degreesForDenseNodeWithPartiallyDeletedRelGroupChain() throws Exception
    {
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.OUT );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.LOOP );

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.OUT );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.OUT, TestRelType.LOOP );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.LOOP );

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType.IN, TestRelType.OUT,
                TestRelType.LOOP );
    }

    @Test
    public void degreesForDenseNodeWithPartiallyDeletedRelChains() throws Exception
    {
        testDegreesForDenseNodeWithPartiallyDeletedRelChains( false, false, false );

        testDegreesForDenseNodeWithPartiallyDeletedRelChains( true, false, false );
        testDegreesForDenseNodeWithPartiallyDeletedRelChains( false, true, false );
        testDegreesForDenseNodeWithPartiallyDeletedRelChains( false, false, true );

        testDegreesForDenseNodeWithPartiallyDeletedRelChains( true, true, false );
        testDegreesForDenseNodeWithPartiallyDeletedRelChains( true, true, true );
        testDegreesForDenseNodeWithPartiallyDeletedRelChains( true, false, true );

        testDegreesForDenseNodeWithPartiallyDeletedRelChains( true, true, true );
    }

    private void testRelationshipTypesForDenseNode( LongConsumer nodeChanger, Set<TestRelType> expectedTypes )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        nodeChanger.accept( nodeId );

        StoreSingleNodeCursor cursor = newCursor( nodeId );

        assertEquals( expectedTypes, relTypes( cursor ) );
    }

    private void testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType... typesToDelete )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        StoreSingleNodeCursor cursor = newCursor( nodeId );

        for ( TestRelType type : typesToDelete )
        {
            markRelGroupNotInUse( nodeId, type );
            switch ( type )
            {
            case IN:
                inRelCount = 0;
                break;
            case OUT:
                outRelCount = 0;
                break;
            case LOOP:
                loopRelCount = 0;
                break;
            default:
                throw new IllegalArgumentException( "Unknown type: " + type );
            }
        }

        assertEquals( outRelCount + loopRelCount, cursor.degree( OUTGOING ) );
        assertEquals( inRelCount + loopRelCount, cursor.degree( INCOMING ) );
        assertEquals( inRelCount + outRelCount + loopRelCount, cursor.degree( BOTH ) );
    }

    private void testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( boolean modifyInChain,
            boolean modifyOutChain, boolean modifyLoopChain )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        StoreSingleNodeCursor cursor = newCursor( nodeId );

        if ( modifyInChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.IN );
        }
        if ( modifyOutChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.OUT );
        }
        if ( modifyLoopChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.LOOP );
        }

        assertEquals( outRelCount + loopRelCount, cursor.degree( OUTGOING ) );
        assertEquals( inRelCount + loopRelCount, cursor.degree( INCOMING ) );
        assertEquals( inRelCount + outRelCount + loopRelCount, cursor.degree( BOTH ) );
    }

    private void testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(
            TestRelType... typesToDelete ) throws Exception
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        StoreSingleNodeCursor cursor = newCursor( nodeId );

        for ( TestRelType type : typesToDelete )
        {
            markRelGroupNotInUse( nodeId, type );
            switch ( type )
            {
            case IN:
                inRelCount = 0;
                break;
            case OUT:
                outRelCount = 0;
                break;
            case LOOP:
                loopRelCount = 0;
                break;
            default:
                throw new IllegalArgumentException( "Unknown type: " + type );
            }
        }

        assertEquals( 0, cursor.degree( OUTGOING, relTypeId( TestRelType.IN ) ) );
        assertEquals( outRelCount, cursor.degree( OUTGOING, relTypeId( TestRelType.OUT ) ) );
        assertEquals( loopRelCount, cursor.degree( OUTGOING, relTypeId( TestRelType.LOOP ) ) );

        assertEquals( 0, cursor.degree( INCOMING, relTypeId( TestRelType.OUT ) ) );
        assertEquals( inRelCount, cursor.degree( INCOMING, relTypeId( TestRelType.IN ) ) );
        assertEquals( loopRelCount, cursor.degree( INCOMING, relTypeId( TestRelType.LOOP ) ) );

        assertEquals( inRelCount, cursor.degree( BOTH, relTypeId( TestRelType.IN ) ) );
        assertEquals( outRelCount, cursor.degree( BOTH, relTypeId( TestRelType.OUT ) ) );
        assertEquals( loopRelCount, cursor.degree( BOTH, relTypeId( TestRelType.LOOP ) ) );
    }

    private void testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( boolean modifyInChain,
            boolean modifyOutChain, boolean modifyLoopChain )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        StoreSingleNodeCursor cursor = newCursor( nodeId );

        if ( modifyInChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.IN );
        }
        if ( modifyOutChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.OUT );
        }
        if ( modifyLoopChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.LOOP );
        }

        assertEquals( 0, cursor.degree( OUTGOING, relTypeId( TestRelType.IN ) ) );
        assertEquals( outRelCount, cursor.degree( OUTGOING, relTypeId( TestRelType.OUT ) ) );
        assertEquals( loopRelCount, cursor.degree( OUTGOING, relTypeId( TestRelType.LOOP ) ) );

        assertEquals( 0, cursor.degree( INCOMING, relTypeId( TestRelType.OUT ) ) );
        assertEquals( inRelCount, cursor.degree( INCOMING, relTypeId( TestRelType.IN ) ) );
        assertEquals( loopRelCount, cursor.degree( INCOMING, relTypeId( TestRelType.LOOP ) ) );

        assertEquals( inRelCount, cursor.degree( BOTH, relTypeId( TestRelType.IN ) ) );
        assertEquals( outRelCount, cursor.degree( BOTH, relTypeId( TestRelType.OUT ) ) );
        assertEquals( loopRelCount, cursor.degree( BOTH, relTypeId( TestRelType.LOOP ) ) );
    }

    private void testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType... typesToDelete )
            throws Exception
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        StoreSingleNodeCursor cursor = newCursor( nodeId );

        for ( TestRelType type : typesToDelete )
        {
            markRelGroupNotInUse( nodeId, type );
            switch ( type )
            {
            case IN:
                inRelCount = 0;
                break;
            case OUT:
                outRelCount = 0;
                break;
            case LOOP:
                loopRelCount = 0;
                break;
            default:
                throw new IllegalArgumentException( "Unknown type: " + type );
            }
        }

        Set<TestDegreeItem> expectedDegrees = new HashSet<>();
        if ( outRelCount != 0 )
        {
            expectedDegrees.add( new TestDegreeItem( relTypeId( TestRelType.OUT ), outRelCount, 0 ) );
        }
        if ( inRelCount != 0 )
        {
            expectedDegrees.add( new TestDegreeItem( relTypeId( TestRelType.IN ), 0, inRelCount ) );
        }
        if ( loopRelCount != 0 )
        {
            expectedDegrees.add( new TestDegreeItem( relTypeId( TestRelType.LOOP ), loopRelCount, loopRelCount ) );
        }

        Set<TestDegreeItem> actualDegrees = degrees( cursor );

        assertEquals( expectedDegrees, actualDegrees );
    }

    private void testDegreesForDenseNodeWithPartiallyDeletedRelChains( boolean modifyInChain, boolean modifyOutChain,
            boolean modifyLoopChain )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        StoreSingleNodeCursor cursor = newCursor( nodeId );

        if ( modifyInChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.IN );
        }
        if ( modifyOutChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.OUT );
        }
        if ( modifyLoopChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, TestRelType.LOOP );
        }

        Set<TestDegreeItem> expectedDegrees = new HashSet<>( asList(
                new TestDegreeItem( relTypeId( TestRelType.OUT ), outRelCount, 0 ),
                new TestDegreeItem( relTypeId( TestRelType.IN ), 0, inRelCount ),
                new TestDegreeItem( relTypeId( TestRelType.LOOP ), loopRelCount, loopRelCount )
        ) );

        Set<TestDegreeItem> actualDegrees = degrees( cursor );

        assertEquals( expectedDegrees, actualDegrees );
    }

    private Set<TestRelType> relTypes( StoreSingleNodeCursor cursor )
    {
        Set<TestRelType> types = new HashSet<>();

        Cursor<IntSupplier> relTypesCursor = cursor.relationshipTypes();
        while ( relTypesCursor.next() )
        {
            int typeId = relTypesCursor.get().getAsInt();
            types.add( relTypeForId( typeId ) );
        }

        return types;
    }

    private TestRelType relTypeForId( int id )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        RelationshipTypeTokenHolder relTypeHolder = resolver.resolveDependency( RelationshipTypeTokenHolder.class );
        try
        {
            String typeName = relTypeHolder.getTokenById( id ).name();
            return TestRelType.valueOf( typeName );
        }
        catch ( TokenNotFoundException e )
        {
            throw new RuntimeException( e );
        }
    }

    private int relTypeId( TestRelType type )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        RelationshipTypeTokenHolder relTypeHolder = resolver.resolveDependency( RelationshipTypeTokenHolder.class );
        int id = relTypeHolder.getIdByName( type.name() );
        assertNotEquals( NO_ID, id );
        return id;
    }

    private Set<TestDegreeItem> degrees( StoreSingleNodeCursor cursor )
    {
        Set<TestDegreeItem> degrees = new HashSet<>();

        Cursor<DegreeItem> degreesCursor = cursor.degrees();
        while ( degreesCursor.next() )
        {
            degrees.add( new TestDegreeItem( degreesCursor.get() ) );
        }

        return degrees;
    }

    private long createNode( int inRelCount, int outRelCount, int loopRelCount )
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            for ( int i = 0; i < inRelCount; i++ )
            {
                Node start = db.createNode();
                start.createRelationshipTo( node, TestRelType.IN );
            }
            for ( int i = 0; i < outRelCount; i++ )
            {
                Node end = db.createNode();
                node.createRelationshipTo( end, TestRelType.OUT );
            }
            for ( int i = 0; i < loopRelCount; i++ )
            {
                node.createRelationshipTo( node, TestRelType.LOOP );
            }
            tx.success();
        }
        return node.getId();
    }

    private void markRelGroupNotInUse( long nodeId, TestRelType... types )
    {
        NodeRecord node = getNodeRecord( nodeId );
        assertTrue( node.isDense() );

        Set<TestRelType> typesToRemove = asSet( types );

        long relGroupId = node.getNextRel();
        while ( relGroupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord relGroup = getRelGroupRecord( relGroupId );
            TestRelType type = relTypeForId( relGroup.getType() );

            if ( typesToRemove.contains( type ) )
            {
                relGroup.setInUse( false );
                update( relGroup );
            }

            relGroupId = relGroup.getNext();
        }
    }

    private void markRandomRelsNotInUse( long nodeId )
    {
        for ( TestRelType type : TestRelType.values() )
        {
            markRandomRelsInGroupNotInUse( nodeId, type );
        }
    }

    private void markRandomRelsInGroupNotInUse( long nodeId, TestRelType type )
    {
        NodeRecord node = getNodeRecord( nodeId );
        assertTrue( node.isDense() );

        long relGroupId = node.getNextRel();
        while ( relGroupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord relGroup = getRelGroupRecord( relGroupId );

            if ( type == relTypeForId( relGroup.getType() ) )
            {
                markRandomRelsInChainNotInUse( relGroup.getFirstOut() );
                markRandomRelsInChainNotInUse( relGroup.getFirstIn() );
                markRandomRelsInChainNotInUse( relGroup.getFirstLoop() );
                return;
            }

            relGroupId = relGroup.getNext();
        }

        throw new IllegalStateException( "No relationship group with type: " + type + " found" );
    }

    private void markRandomRelsInChainNotInUse( long relId )
    {
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord record = getRelRecord( relId );

            boolean shouldBeMarked = random.nextBoolean();
            if ( shouldBeMarked )
            {
                record.setInUse( false );
                update( record );
            }

            markRandomRelsInChainNotInUse( record.getFirstNextRel() );
            boolean isLoopRelationship = record.getFirstNextRel() == record.getSecondNextRel();
            if ( !isLoopRelationship )
            {
                markRandomRelsInChainNotInUse( record.getSecondNextRel() );
            }
        }
    }

    private void noNodeChange( long nodeId )
    {
    }

    @SuppressWarnings( "unchecked" )
    private StoreSingleNodeCursor newCursor( long nodeId )
    {
        StoreSingleNodeCursor cursor = new StoreSingleNodeCursor( new NodeRecord( -1 ), resolveNeoStores(),
                mock( StoreStatement.class ), mock( Consumer.class ), new RecordCursors( resolveNeoStores() ),
                NO_LOCK_SERVICE );

        cursor.init( nodeId );
        assertTrue( cursor.next() );

        return cursor;
    }

    private static <R extends AbstractBaseRecord> R getRecord( RecordStore<R> store, long id )
    {
        return RecordStore.getRecord( store, id, RecordLoad.FORCE );
    }

    private NodeRecord getNodeRecord( long id )
    {
        return getRecord( resolveNeoStores().getNodeStore(), id );
    }

    private RelationshipRecord getRelRecord( long id )
    {
        return getRecord( resolveNeoStores().getRelationshipStore(), id );
    }

    private void update( RelationshipRecord record )
    {
        resolveNeoStores().getRelationshipStore().updateRecord( record );
    }

    private RelationshipGroupRecord getRelGroupRecord( long id )
    {
        return getRecord( resolveNeoStores().getRelationshipGroupStore(), id );
    }

    private void update( RelationshipGroupRecord record )
    {
        resolveNeoStores().getRelationshipGroupStore().updateRecord( record );
    }

    private NeoStores resolveNeoStores()
    {
        DependencyResolver resolver = db.getDependencyResolver();
        RecordStorageEngine storageEngine = resolver.resolveDependency( RecordStorageEngine.class );
        return storageEngine.testAccessNeoStores();
    }

    private int randomRelCount()
    {
        return RELATIONSHIPS_COUNT + random.nextInt( 20 );
    }

    private enum TestRelType implements RelationshipType
    {
        IN,
        OUT,
        LOOP
    }
}
