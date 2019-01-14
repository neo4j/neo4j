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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyKeyTokenRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

public class IdGeneratorTest
{
    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private TestDirectory testDirectory = TestDirectory.testDirectory(fsRule.get());

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( testDirectory );

    private EphemeralFileSystemAbstraction fs;

    @Before
    public void doBefore()
    {
        fs = fsRule.get();
    }

    private void deleteIdGeneratorFile()
    {
        fs.deleteFile( idGeneratorFile() );
    }

    private File idGeneratorFile()
    {
        return testDirectory.file( "testIdGenerator.id" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void cannotCreateIdGeneratorWithNullFileSystem()
    {
        IdGeneratorImpl.createGenerator( null, idGeneratorFile(), 0, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void cannotCreateIdGeneratorWithNullFile()
    {
        IdGeneratorImpl.createGenerator( fs, null, 0, false );
    }

    @Test( expected = IllegalArgumentException.class )
    public void grabSizeCannotBeZero()
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        new IdGeneratorImpl( fs, idGeneratorFile(), 0, 100, false, IdType.NODE, () -> 0L ).close();
    }

    @Test( expected = IllegalArgumentException.class )
    public void grabSizeCannotBeNegative()
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        new IdGeneratorImpl( fs, idGeneratorFile(), -1, 100, false, IdType.NODE, () -> 0L ).close();
    }

    @Test( expected = IllegalStateException.class )
    public void createIdGeneratorMustRefuseOverwritingExistingFile() throws IOException
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1008, 1000, false, IdType.NODE, () -> 0L );
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, true );
        }
        finally
        {
            closeIdGenerator( idGenerator );
            // verify that id generator is ok
            StoreChannel fileChannel = fs.open( idGeneratorFile(), OpenMode.READ_WRITE );
            ByteBuffer buffer = ByteBuffer.allocate( 9 );
            fileChannel.readAll( buffer );
            buffer.flip();
            assertEquals( (byte) 0, buffer.get() );
            assertEquals( 0L, buffer.getLong() );
            buffer.flip();
            int readCount = fileChannel.read( buffer );
            if ( readCount != -1 && readCount != 0 )
            {
                fail( "Id generator header not ok read 9 + " + readCount + " bytes from file" );
            }
            fileChannel.close();

            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    private void closeIdGenerator( IdGenerator idGenerator )
    {
        idGenerator.close();
    }

    @Test
    public void mustOverwriteExistingFileIfRequested()
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1008, 1000, false, IdType.NODE, () -> 0L );
        long[] firstFirstIds = new long[]{idGenerator.nextId(), idGenerator.nextId(), idGenerator.nextId()};
        idGenerator.close();

        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1008, 1000, false, IdType.NODE, () -> 0L );
        long[] secondFirstIds = new long[]{idGenerator.nextId(), idGenerator.nextId(), idGenerator.nextId()};
        idGenerator.close();

        // Basically, recreating the id file should be the same as start over with the ids.
        assertThat( secondFirstIds, is( firstFirstIds ) );
    }

    @Test
    public void testStickyGenerator()
    {
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGen = new IdGeneratorImpl( fs, idGeneratorFile(), 3, 1000, false, IdType.NODE, () -> 0L );
            try
            {
                new IdGeneratorImpl( fs, idGeneratorFile(), 3, 1000, false, IdType.NODE, () -> 0L );
                fail( "Opening sticky id generator should throw exception" );
            }
            catch ( StoreFailureException e )
            { // good
            }
            closeIdGenerator( idGen );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testNextId()
    {
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 3, 1000, false, IdType.NODE, () -> 0L );
            for ( long i = 0; i < 7; i++ )
            {
                assertEquals( i, idGenerator.nextId() );
            }
            idGenerator.freeId( 1 );
            idGenerator.freeId( 3 );
            idGenerator.freeId( 5 );
            assertEquals( 7L, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 5, 1000, false, IdType.NODE, () -> 0L );
            idGenerator.freeId( 2 );
            idGenerator.freeId( 4 );
            assertEquals( 1L, idGenerator.nextId() );
            idGenerator.freeId( 1 );
            assertEquals( 3L, idGenerator.nextId() );
            idGenerator.freeId( 3 );
            assertEquals( 5L, idGenerator.nextId() );
            idGenerator.freeId( 5 );
            assertEquals( 6L, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            assertEquals( 8L, idGenerator.nextId() );
            idGenerator.freeId( 8 );
            assertEquals( 9L, idGenerator.nextId() );
            idGenerator.freeId( 9 );
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 3, 1000, false, IdType.NODE, () -> 0L );
            assertEquals( 6L, idGenerator.nextId() );
            assertEquals( 8L, idGenerator.nextId() );
            assertEquals( 9L, idGenerator.nextId() );
            assertEquals( 1L, idGenerator.nextId() );
            assertEquals( 3L, idGenerator.nextId() );
            assertEquals( 5L, idGenerator.nextId() );
            assertEquals( 2L, idGenerator.nextId() );
            assertEquals( 4L, idGenerator.nextId() );
            assertEquals( 10L, idGenerator.nextId() );
            assertEquals( 11L, idGenerator.nextId() );
            closeIdGenerator( idGenerator );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testFreeId()
    {
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 3, 1000, false, IdType.NODE, () -> 0L );
            for ( long i = 0; i < 7; i++ )
            {
                assertEquals( i, idGenerator.nextId() );
            }
            try
            {
                idGenerator.freeId( -1 );
                fail( "Negative id should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            try
            {
                idGenerator.freeId( 7 );
                fail( "Greater id than ever returned should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            for ( int i = 0; i < 7; i++ )
            {
                idGenerator.freeId( i );
            }
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 2, 1000, false, IdType.NODE, () -> 0L );
            assertEquals( 5L, idGenerator.nextId() );
            assertEquals( 6L, idGenerator.nextId() );
            assertEquals( 3L, idGenerator.nextId() );
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 30, 1000, false, IdType.NODE, () -> 0L );

            assertEquals( 0L, idGenerator.nextId() );
            assertEquals( 1L, idGenerator.nextId() );
            assertEquals( 2L, idGenerator.nextId() );
            assertEquals( 4L, idGenerator.nextId() );
            closeIdGenerator( idGenerator );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testClose()
    {
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 2, 1000, false, IdType.NODE, () -> 0L );
            closeIdGenerator( idGenerator );
            try
            {
                idGenerator.nextId();
                fail( "nextId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            try
            {
                idGenerator.freeId( 0 );
                fail( "freeId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 2, 1000, false, IdType.NODE, () -> 0L );
            assertEquals( 0L, idGenerator.nextId() );
            assertEquals( 1L, idGenerator.nextId() );
            assertEquals( 2L, idGenerator.nextId() );
            closeIdGenerator( idGenerator );
            try
            {
                idGenerator.nextId();
                fail( "nextId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            try
            {
                idGenerator.freeId( 0 );
                fail( "freeId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testOddAndEvenWorstCase()
    {
        int capacity = 1024 * 8 + 1;
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 128, capacity * 2, false, IdType.NODE, () -> 0L );
            for ( int i = 0; i < capacity; i++ )
            {
                idGenerator.nextId();
            }
            Map<Long, Object> freedIds = new HashMap<>();
            for ( long i = 1; i < capacity; i += 2 )
            {
                idGenerator.freeId( i );
                freedIds.put( i, this );
            }
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 2000, capacity * 2, false, IdType.NODE, () -> 0L );
            long oldId = -1;
            for ( int i = 0; i < capacity - 1; i += 2 )
            {
                long id = idGenerator.nextId();
                if ( freedIds.remove( id ) == null )
                {
                    throw new RuntimeException( "Id=" + id + " prevId=" + oldId + " list.size()=" + freedIds.size() );
                }
                oldId = id;
            }
            assertTrue( freedIds.values().size() == 0 );
            closeIdGenerator( idGenerator );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( fs.fileExists( file ) )
            {
                assertTrue( fs.deleteFile( file ) );
            }
        }
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 128, capacity * 2, false, IdType.NODE, () -> 0L );
            for ( int i = 0; i < capacity; i++ )
            {
                idGenerator.nextId();
            }
            Map<Long, Object> freedIds = new HashMap<>();
            for ( long i = 0; i < capacity; i += 2 )
            {
                idGenerator.freeId( i );
                freedIds.put( i, this );
            }
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 2000, capacity * 2, false, IdType.NODE, () -> 0L );
            for ( int i = 0; i < capacity; i += 2 )
            {
                assertEquals( this, freedIds.remove( idGenerator.nextId() ) );
            }
            assertEquals( 0, freedIds.values().size() );
            closeIdGenerator( idGenerator );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testRandomTest()
    {
        java.util.Random random = new java.util.Random( System.currentTimeMillis() );
        int capacity = random.nextInt( 1024 ) + 1024;
        int grabSize = random.nextInt( 128 ) + 128;
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), grabSize, capacity * 2, false, IdType.NODE, () -> 0L );
        List<Long> idsTaken = new ArrayList<>();
        float releaseIndex = 0.25f;
        float closeIndex = 0.05f;
        int currentIdCount = 0;
        try
        {
            while ( currentIdCount < capacity )
            {
                float rIndex = random.nextFloat();
                if ( rIndex < releaseIndex && currentIdCount > 0 )
                {
                    idGenerator.freeId( idsTaken.remove( random.nextInt( currentIdCount ) ).intValue() );
                    currentIdCount--;
                }
                else
                {
                    idsTaken.add( idGenerator.nextId() );
                    currentIdCount++;
                }
                if ( rIndex > (1.0f - closeIndex) || rIndex < closeIndex )
                {
                    closeIdGenerator( idGenerator );
                    grabSize = random.nextInt( 128 ) + 128;
                    idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), grabSize, capacity * 2, false, IdType.NODE, () -> 0L );
                }
            }
            closeIdGenerator( idGenerator );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testUnsignedId()
    {
        try
        {
            PropertyKeyTokenRecordFormat recordFormat = new PropertyKeyTokenRecordFormat();
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1,
                    recordFormat.getMaxId(), false, IdType.NODE, () -> 0L );
            idGenerator.setHighId( recordFormat.getMaxId() );
            long id = idGenerator.nextId();
            assertEquals( recordFormat.getMaxId(), id );
            idGenerator.freeId( id );
            try
            {
                idGenerator.nextId();
                fail( "Shouldn't be able to get next ID" );
            }
            catch ( StoreFailureException e )
            { // good, capacity exceeded
            }
            closeIdGenerator( idGenerator );
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1, recordFormat.getMaxId(), false, IdType.NODE, () -> 0L );
            assertEquals( recordFormat.getMaxId() + 1, idGenerator.getHighId() );
            id = idGenerator.nextId();
            assertEquals( recordFormat.getMaxId(), id );
            try
            {
                idGenerator.nextId();
            }
            catch ( StoreFailureException e )
            { // good, capacity exceeded
            }
            closeIdGenerator( idGenerator );
        }
        finally
        {
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void makeSureIdCapacityCannotBeExceeded()
    {
        RecordFormats formats = Standard.LATEST_RECORD_FORMATS;
        List<RecordFormat<? extends AbstractBaseRecord>> recordFormats = Arrays.asList( formats.node(),
                formats.dynamic(),
                formats.labelToken(),
                formats.property(),
                formats.propertyKeyToken(),
                formats.relationship(),
                formats.relationshipGroup(),
                formats.relationshipTypeToken() );

        for ( RecordFormat format : recordFormats )
        {
            makeSureIdCapacityCannotBeExceeded( format );
        }
    }

    private void makeSureIdCapacityCannotBeExceeded( RecordFormat format )
    {
        deleteIdGeneratorFile();
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        long maxValue = format.getMaxId();
        IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1, maxValue - 1, false, IdType.NODE, () -> 0L );
        long id = maxValue - 2;
        idGenerator.setHighId( id );
        assertEquals( id, idGenerator.nextId() );
        assertEquals( id + 1, idGenerator.nextId() );
        try
        {
            idGenerator.nextId();
            fail( "Id capacity shouldn't be able to be exceeded for " + format );
        }
        catch ( StoreFailureException e )
        { // Good
        }
        closeIdGenerator( idGenerator );
    }

    @Test
    public void makeSureMagicMinusOneIsNotReturnedFromNodeIdGenerator()
    {
        makeSureMagicMinusOneIsSkipped( new NodeRecordFormat() );
        makeSureMagicMinusOneIsSkipped( new RelationshipRecordFormat() );
        makeSureMagicMinusOneIsSkipped( new PropertyRecordFormat());
    }

    private void makeSureMagicMinusOneIsSkipped( RecordFormat format )
    {
        deleteIdGeneratorFile();
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1, format.getMaxId(), false, IdType.NODE, () -> 0L );
        long id = (long) Math.pow( 2, 32 ) - 3;
        idGenerator.setHighId( id );
        assertEquals( id, idGenerator.nextId() );
        assertEquals( id + 1, idGenerator.nextId() );
        // Here we make sure that id+2 (integer -1) is skipped
        assertEquals( id + 3, idGenerator.nextId() );
        assertEquals( id + 4, idGenerator.nextId() );
        assertEquals( id + 5, idGenerator.nextId() );
        closeIdGenerator( idGenerator );
    }

    @Test
    public void makeSureMagicMinusOneCannotBeReturnedEvenIfFreed()
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        IdGenerator idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1, new NodeRecordFormat().getMaxId(), false, IdType.NODE, () -> 0L );
        long magicMinusOne = (long) Math.pow( 2, 32 ) - 1;
        idGenerator.setHighId( magicMinusOne );
        assertEquals( magicMinusOne + 1, idGenerator.nextId() );
        idGenerator.freeId( magicMinusOne - 1 );
        idGenerator.freeId( magicMinusOne );
        closeIdGenerator( idGenerator );

        idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 1, new NodeRecordFormat().getMaxId(), false, IdType.NODE, () -> 0L );
        assertEquals( magicMinusOne - 1, idGenerator.nextId() );
        assertEquals( magicMinusOne + 2, idGenerator.nextId() );
        closeIdGenerator( idGenerator );
    }

    @Test
    public void commandsGetWrittenOnceSoThatFreedIdsGetsAddedOnlyOnce() throws Exception
    {
        File storeDir = new File( "target/var/free-id-once" );
        deleteRecursively( storeDir );
        GraphDatabaseService db = createTestDatabase( storeDir );
        RelationshipType type = withName( "SOME_TYPE" );

        // This transaction will, if some commands may be executed more than
        // once,
        // add the freed ids to the defrag list more than once - making the id
        // generator
        // return the same id more than once during the next session.
        Set<Long> createdNodeIds = new HashSet<>();
        Set<Long> createdRelationshipIds = new HashSet<>();
        Transaction tx = db.beginTx();
        Node commonNode = db.createNode();
        for ( int i = 0; i < 20; i++ )
        {
            Node otherNode = db.createNode();
            Relationship relationship = commonNode.createRelationshipTo( otherNode, type );
            if ( i % 5 == 0 )
            {
                otherNode.delete();
                relationship.delete();
            }
            else
            {
                createdNodeIds.add( otherNode.getId() );
                createdRelationshipIds.add( relationship.getId() );
            }
        }
        tx.success();
        tx.close();
        db.shutdown();

        // After a clean shutdown, create new nodes and relationships and see so
        // that
        // all ids are unique.
        db = createTestDatabase( storeDir );
        tx = db.beginTx();
        commonNode = db.getNodeById( commonNode.getId() );
        for ( int i = 0; i < 100; i++ )
        {
            Node otherNode = db.createNode();
            if ( !createdNodeIds.add( otherNode.getId() ) )
            {
                fail( "Managed to create a node with an id that was already in use" );
            }
            Relationship relationship = commonNode.createRelationshipTo( otherNode, type );
            if ( !createdRelationshipIds.add( relationship.getId() ) )
            {
                fail( "Managed to create a relationship with an id that was already in use" );
            }
        }
        tx.success();
        tx.close();

        // Verify by loading everything from scratch
        tx = db.beginTx();
        for ( Node node : db.getAllNodes() )
        {
            Iterables.lastOrNull( node.getRelationships() );
        }
        tx.close();
        db.shutdown();
    }

    @Test
    public void delete()
    {
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        IdGeneratorImpl idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 10, 1000, false, IdType.NODE, () -> 0L );
        long id = idGenerator.nextId();
        idGenerator.nextId();
        idGenerator.freeId( id );
        idGenerator.close();
        idGenerator.delete();
        assertFalse( idGeneratorFile().exists() );
        IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
        idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), 10, 1000, false, IdType.NODE, () -> 0L );
        assertEquals( id, idGenerator.nextId() );
        idGenerator.close();
    }

    @Test
    public void testChurnIdBatchAtGrabSize()
    {
        IdGenerator idGenerator = null;
        try
        {
            IdGeneratorImpl.createGenerator( fs, idGeneratorFile(), 0, false );
            final int grabSize = 10;
            int rounds = 10;
            idGenerator = new IdGeneratorImpl( fs, idGeneratorFile(), grabSize, 1000, true, IdType.NODE, () -> 0L );

            for ( int i = 0; i < rounds; i++ )
            {
                Set<Long> ids = new HashSet<>();
                for ( int j = 0; j < grabSize; j++ )
                {
                    ids.add( idGenerator.nextId() );
                }
                for ( Long id : ids )
                {
                    idGenerator.freeId( id );
                }
            }
            long newId = idGenerator.nextId();
            assertTrue( "Expected IDs to be reused (" + grabSize + " at a time). high ID was: " + newId,
                    newId < grabSize * rounds );
        }
        finally
        {
            if ( idGenerator != null )
            {
                closeIdGenerator( idGenerator );
            }
            File file = idGeneratorFile();
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    private GraphDatabaseService createTestDatabase( File storeDir )
    {
        return new TestGraphDatabaseFactory()
                .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                .newImpermanentDatabase( storeDir );
    }
}
