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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongConsumer;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.RandomRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countAll;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countIncoming;
import static org.neo4j.internal.kernel.api.helpers.Nodes.countOutgoing;
import static org.neo4j.kernel.impl.core.TokenHolder.NO_ID;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.TestRelType.IN;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.TestRelType.LOOP;
import static org.neo4j.kernel.impl.storageengine.impl.recordstorage.TestRelType.OUT;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

public class RecordStorageReaderRelTypesAndDegreeTest extends RecordStorageReaderTestBase
{
    private static final int RELATIONSHIPS_COUNT = 20;

    @Rule
    public final RandomRule random = new RandomRule();

    @Override
    protected GraphDatabaseService createGraphDatabase()
    {
        return new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.dense_node_threshold, String.valueOf( RELATIONSHIPS_COUNT ) )
                .newGraphDatabase();
    }

    @Test
    public void degreesForDenseNodeWithPartiallyDeletedRelGroupChain()
    {
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( IN );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( OUT );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( LOOP );

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( IN, OUT );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( OUT, LOOP );
        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( IN, LOOP );

        testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( IN, OUT, LOOP );
    }

    @Test
    public void degreesForDenseNodeWithPartiallyDeletedRelChains()
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

    @Test
    public void degreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain()
    {
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( IN );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( OUT );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( LOOP );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( IN, OUT );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( IN, LOOP );
        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( OUT, LOOP );

        testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( IN, OUT,
                LOOP );
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
    public void degreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain()
    {
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain();

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( IN );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( OUT );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( LOOP );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( IN, OUT );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( OUT, LOOP );
        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( IN, LOOP );

        testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain( IN, OUT,
                LOOP );
    }

    @Test
    public void degreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains()
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

    private void testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType... typesToDelete )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        NodeCursor nodeCursor = newCursor( nodeId );

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

        assertEquals( outRelCount + loopRelCount, countOutgoing( nodeCursor, storageReader ) );
        assertEquals( inRelCount + loopRelCount, countIncoming( nodeCursor, storageReader ) );
        assertEquals( inRelCount + outRelCount + loopRelCount, Nodes.countAll( nodeCursor, storageReader ) );
    }

    private void testDegreeByDirectionForDenseNodeWithPartiallyDeletedRelChains( boolean modifyInChain,
            boolean modifyOutChain, boolean modifyLoopChain )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        NodeCursor nodeCursor = newCursor( nodeId );

        if ( modifyInChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, IN );
        }
        if ( modifyOutChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, OUT );
        }
        if ( modifyLoopChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, LOOP );
        }

        assertEquals( outRelCount + loopRelCount, countOutgoing( nodeCursor, storageReader ) );
        assertEquals( inRelCount + loopRelCount, countIncoming( nodeCursor, storageReader ) );
        assertEquals( inRelCount + outRelCount + loopRelCount, Nodes.countAll( nodeCursor, storageReader ) );
    }

    private void testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelGroupChain(
            TestRelType... typesToDelete )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        NodeCursor nodeCursor = newCursor( nodeId );

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

        assertEquals( 0, countOutgoing( nodeCursor, storageReader, relTypeId( IN ) ) );
        assertEquals( outRelCount, countOutgoing( nodeCursor, storageReader, relTypeId( OUT ) ) );
        assertEquals( loopRelCount, countOutgoing( nodeCursor, storageReader, relTypeId( LOOP ) ) );

        assertEquals( 0, countIncoming( nodeCursor, storageReader, relTypeId( OUT ) ) );
        assertEquals( inRelCount, countIncoming( nodeCursor, storageReader, relTypeId( IN ) ) );
        assertEquals( loopRelCount, countIncoming( nodeCursor, storageReader, relTypeId( LOOP ) ) );

        assertEquals( inRelCount, countAll( nodeCursor, storageReader, relTypeId( IN ) ) );
        assertEquals( outRelCount, countAll( nodeCursor, storageReader, relTypeId( OUT ) ) );
        assertEquals( loopRelCount, countAll( nodeCursor, storageReader, relTypeId( LOOP ) ) );
    }

    private void testDegreeByDirectionAndTypeForDenseNodeWithPartiallyDeletedRelChains( boolean modifyInChain,
            boolean modifyOutChain, boolean modifyLoopChain )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        NodeCursor nodeCursor = newCursor( nodeId );

        if ( modifyInChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, IN );
        }
        if ( modifyOutChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, OUT );
        }
        if ( modifyLoopChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, LOOP );
        }

        assertEquals( 0, countOutgoing( nodeCursor, storageReader, relTypeId( IN ) ) );
        assertEquals( outRelCount, countOutgoing( nodeCursor, storageReader, relTypeId( OUT ) ) );
        assertEquals( loopRelCount, countOutgoing( nodeCursor, storageReader, relTypeId( LOOP ) ) );

        assertEquals( 0, countIncoming( nodeCursor, storageReader, relTypeId( OUT ) ) );
        assertEquals( inRelCount, countIncoming( nodeCursor, storageReader, relTypeId( IN ) ) );
        assertEquals( loopRelCount, countIncoming( nodeCursor, storageReader, relTypeId( LOOP ) ) );

        assertEquals( inRelCount, countAll( nodeCursor, storageReader, relTypeId( IN ) ) );
        assertEquals( outRelCount, countAll( nodeCursor, storageReader, relTypeId( OUT ) ) );
        assertEquals( loopRelCount, countAll( nodeCursor, storageReader, relTypeId( LOOP ) ) );
    }

    @Test
    public void relationshipTypesForDenseNodeWithPartiallyDeletedRelGroupChain()
    {
        testRelationshipTypesForDenseNode( this::noNodeChange,
                asSet( TestRelType.IN, TestRelType.OUT, TestRelType.LOOP ) );

        testRelationshipTypesForDenseNode( nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN ),
                asSet( TestRelType.OUT, TestRelType.LOOP ) );
        testRelationshipTypesForDenseNode( nodeId -> markRelGroupNotInUse( nodeId, TestRelType.OUT ),
                asSet( TestRelType.IN, TestRelType.LOOP ) );
        testRelationshipTypesForDenseNode( nodeId -> markRelGroupNotInUse( nodeId, TestRelType.LOOP ),
                asSet( TestRelType.IN, TestRelType.OUT ) );

        testRelationshipTypesForDenseNode( nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN, TestRelType.OUT ),
                asSet( TestRelType.LOOP ) );
        testRelationshipTypesForDenseNode( nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN, TestRelType.LOOP ),
                asSet( TestRelType.OUT ) );
        testRelationshipTypesForDenseNode( nodeId -> markRelGroupNotInUse( nodeId, TestRelType.OUT, TestRelType.LOOP ),
                asSet( TestRelType.IN ) );

        testRelationshipTypesForDenseNode(
                nodeId -> markRelGroupNotInUse( nodeId, TestRelType.IN, TestRelType.OUT, TestRelType.LOOP ),
                emptySet() );
    }

    @Test
    public void relationshipTypesForDenseNodeWithPartiallyDeletedRelChains()
    {
        testRelationshipTypesForDenseNode( this::markRandomRelsNotInUse,
                asSet( TestRelType.IN, TestRelType.OUT, TestRelType.LOOP ) );
    }

    private void testRelationshipTypesForDenseNode( LongConsumer nodeChanger, Set<TestRelType> expectedTypes )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        nodeChanger.accept( nodeId );

        NodeCursor nodeCursor = newCursor( nodeId );

        assertEquals( expectedTypes, relTypes( nodeCursor ) );
    }

    private Set<TestRelType> relTypes( NodeCursor cursor )
    {
        RelationshipGroupCursor groupCursor = storageReader.allocateRelationshipGroupCursor();
        cursor.relationships( groupCursor );
        Set<TestRelType> result = new HashSet<>();
        while ( groupCursor.next() )
        {
            try
            {
                result.add( TestRelType.valueOf( storageReader.relationshipTypeGetName( groupCursor.type() ) ) );
            }
            catch ( RelationshipTypeIdNotFoundKernelException e )
            {
                throw new RuntimeException( e );
            }
        }
        return result;
    }

    private void testDegreesForDenseNodeWithPartiallyDeletedRelGroupChain( TestRelType... typesToDelete )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        NodeCursor nodeCursor = newCursor( nodeId );

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

        Set<Degrees> expectedDegrees = new HashSet<>();
        if ( outRelCount != 0 )
        {
            expectedDegrees.add( new Degrees( relTypeId( OUT ), outRelCount, 0 ) );
        }
        if ( inRelCount != 0 )
        {
            expectedDegrees.add( new Degrees( relTypeId( IN ), 0, inRelCount ) );
        }
        if ( loopRelCount != 0 )
        {
            expectedDegrees.add( new Degrees( relTypeId( LOOP ), loopRelCount, loopRelCount ) );
        }

        Set<Degrees> actualDegrees = degrees( nodeCursor );

        assertEquals( expectedDegrees, actualDegrees );
    }

    private void testDegreesForDenseNodeWithPartiallyDeletedRelChains( boolean modifyInChain, boolean modifyOutChain,
            boolean modifyLoopChain )
    {
        int inRelCount = randomRelCount();
        int outRelCount = randomRelCount();
        int loopRelCount = randomRelCount();

        long nodeId = createNode( inRelCount, outRelCount, loopRelCount );
        NodeCursor nodeCursor = newCursor( nodeId );

        if ( modifyInChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, IN );
        }
        if ( modifyOutChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, OUT );
        }
        if ( modifyLoopChain )
        {
            markRandomRelsInGroupNotInUse( nodeId, LOOP );
        }

        Set<Degrees> expectedDegrees = new HashSet<>(
                asList( new Degrees( relTypeId( OUT ), outRelCount, 0 ),
                        new Degrees( relTypeId( IN ), 0, inRelCount ),
                        new Degrees( relTypeId( LOOP ), loopRelCount, loopRelCount ) ) );

        Set<Degrees> actualDegrees = degrees( nodeCursor );

        assertEquals( expectedDegrees, actualDegrees );
    }

    private Set<Degrees> degrees( NodeCursor nodeCursor )
    {
        Set<Degrees> degrees = new HashSet<>();
        RelationshipGroupCursor groupCursor = storageReader.allocateRelationshipGroupCursor();
        nodeCursor.relationships( groupCursor );
        while ( groupCursor.next() )
        {
            degrees.add( new Degrees( groupCursor.type(),
                    groupCursor.outgoingCount() + groupCursor.loopCount(),
                    groupCursor.incomingCount() + groupCursor.loopCount() ) );
        }
        return degrees;
    }

    private NodeCursor newCursor( long nodeId )
    {
        NodeCursor nodeCursor = storageReader.allocateNodeCursor();
        storageReader.singleNode( nodeId, nodeCursor );
        nodeCursor.next();
        return nodeCursor;
    }

    private void noNodeChange( long nodeId )
    {
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
        while ( relGroupId != NO_NEXT_RELATIONSHIP.intValue() )
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
        if ( relId != NO_NEXT_RELATIONSHIP.intValue() )
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

    private void markRelGroupNotInUse( long nodeId, TestRelType... types )
    {
        NodeRecord node = getNodeRecord( nodeId );
        assertTrue( node.isDense() );

        Set<TestRelType> typesToRemove = asSet( types );

        long relGroupId = node.getNextRel();
        while ( relGroupId != NO_NEXT_RELATIONSHIP.intValue() )
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

    private int relTypeId( TestRelType type )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        RelationshipTypeTokenHolder relTypeHolder = resolver.resolveDependency( RelationshipTypeTokenHolder.class );
        int id = relTypeHolder.getIdByName( type.name() );
        assertNotEquals( NO_ID, id );
        return id;
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
                start.createRelationshipTo( node, IN );
            }
            for ( int i = 0; i < outRelCount; i++ )
            {
                Node end = db.createNode();
                node.createRelationshipTo( end, OUT );
            }
            for ( int i = 0; i < loopRelCount; i++ )
            {
                node.createRelationshipTo( node, LOOP );
            }
            tx.success();
        }
        return node.getId();
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

    private RelationshipGroupRecord getRelGroupRecord( long id )
    {
        return getRecord( resolveNeoStores().getRelationshipGroupStore(), id );
    }

    private void update( RelationshipGroupRecord record )
    {
        resolveNeoStores().getRelationshipGroupStore().updateRecord( record );
    }

    private void update( RelationshipRecord record )
    {
        resolveNeoStores().getRelationshipStore().updateRecord( record );
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

    private static class Degrees
    {
        private final int type;
        private final int outgoing;
        private final int incoming;

        Degrees( int type, int outgoing, int incoming )
        {
            this.type = type;
            this.outgoing = outgoing;
            this.incoming = incoming;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Degrees degrees = (Degrees) o;
            return type == degrees.type && outgoing == degrees.outgoing && incoming == degrees.incoming;
        }

        @Override
        public int hashCode()
        {

            return Objects.hash( type, outgoing, incoming );
        }
    }
}
