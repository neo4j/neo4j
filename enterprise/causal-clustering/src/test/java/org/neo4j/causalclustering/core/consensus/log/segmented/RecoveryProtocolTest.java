/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.DummyRaftableContentSerializer;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class RecoveryProtocolTest
{
    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    private EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
    private ChannelMarshal<ReplicatedContent> contentMarshal = new DummyRaftableContentSerializer();
    private final File root = new File( "root" );
    private FileNames fileNames = new FileNames( root );
    private SegmentHeader.Marshal headerMarshal = new SegmentHeader.Marshal();
    private ReaderPool readerPool = new ReaderPool( 0, getInstance(), fileNames, fsa, Clocks.fakeClock() );

    @Before
    public void setup()
    {
        fsa.mkdirs( root );
    }

    @Test
    public void shouldReturnEmptyStateOnEmptyDirectory() throws Exception
    {
        // given
        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        // when
        State state = protocol.run();

        // then
        assertEquals( -1, state.appendIndex );
        assertEquals( -1, state.terms.latest() );
        assertEquals( -1, state.prevIndex );
        assertEquals( -1, state.prevTerm );
        assertEquals( 0, state.segments.last().header().version() );
    }

    @Test
    public void shouldFailIfThereAreGapsInVersionNumberSequence() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 5, 2, 2, 5, 0 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        try
        {
            // when
            protocol.run();
            fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailIfTheVersionNumberInTheHeaderAndFileNameDiffer() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 1, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        try
        {
            // when
            protocol.run();
            fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    public void shouldFailIfANonLastFileIsMissingHeader() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createEmptyLogFile( fsa, 1 );
        createLogFile( fsa, -1, 2, 2, -1, -1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        try
        {
            // when
            protocol.run();
            fail( "Expected an exception" );
        }
        catch ( DamagedLogStorageException e )
        {
            // expected
        }
    }

    @Test
    public void shouldRecoverEvenIfLastHeaderIsMissing() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createEmptyLogFile( fsa, 1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        // when
        protocol.run();

        // then
        assertNotEquals( 0, fsa.getFileSize( fileNames.getForVersion( 1 ) ) );
    }

    @Test
    public void shouldRecoverAndBeAbleToRotate() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10,  0 );
        createLogFile( fsa, 20, 2, 2, 20,  1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.rotate( 20, 20, 1 );

        // then
        assertEquals( 20, newFile.header().prevFileLastIndex() );
        assertEquals(  3, newFile.header().version() );
        assertEquals( 20, newFile.header().prevIndex() );
        assertEquals(  1, newFile.header().prevTerm() );
    }

    @Test
    public void shouldRecoverAndBeAbleToTruncate() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10,  0 );
        createLogFile( fsa, 20, 2, 2, 20,  1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.truncate( 20, 15, 0 );

        // then
        assertEquals( 20, newFile.header().prevFileLastIndex() );
        assertEquals(  3, newFile.header().version() );
        assertEquals( 15, newFile.header().prevIndex() );
        assertEquals(  0, newFile.header().prevTerm() );
    }

    @Test
    public void shouldRecoverAndBeAbleToSkip() throws Exception
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, 10, 1, 1, 10,  0 );
        createLogFile( fsa, 20, 2, 2, 20,  1 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        // when
        State state = protocol.run();
        SegmentFile newFile = state.segments.skip( 20, 40, 2 );

        // then
        assertEquals( 20, newFile.header().prevFileLastIndex() );
        assertEquals(  3, newFile.header().version() );
        assertEquals( 40, newFile.header().prevIndex() );
        assertEquals(  2, newFile.header().prevTerm() );
    }

    @Test
    public void shouldRecoverBootstrappedEntry() throws Exception
    {
        for ( int bootstrapIndex = 0; bootstrapIndex < 5; bootstrapIndex++ )
        {
            for ( long bootstrapTerm = 0; bootstrapTerm < 5; bootstrapTerm++ )
            {
                testRecoveryOfBootstrappedEntry( bootstrapIndex, bootstrapTerm );
            }
        }
    }

    private void testRecoveryOfBootstrappedEntry( long bootstrapIndex, long bootstrapTerm )
            throws IOException, DamagedLogStorageException, DisposedException
    {
        // given
        createLogFile( fsa, -1, 0, 0, -1, -1 );
        createLogFile( fsa, -1, 1, 1, bootstrapIndex, bootstrapTerm );

        RecoveryProtocol protocol =
                new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal, NullLogProvider.getInstance() );

        // when
        State state = protocol.run();

        // then
        assertEquals( bootstrapIndex, state.prevIndex );
        assertEquals( bootstrapTerm, state.prevTerm );

        assertEquals( -1, state.terms.get( -1 ) );
        assertEquals( -1, state.terms.get( bootstrapIndex - 1 ) );
        assertEquals( bootstrapTerm, state.terms.get( bootstrapIndex ) );
        assertEquals( -1, state.terms.get( bootstrapIndex + 1 ) );

        assertEquals( bootstrapTerm, state.terms.latest() );
    }

    @Test
    public void shouldRecoverSeveralSkips() throws Exception
    {
        // given
        createLogFile( fsa, 10, 1, 1, 20, 9 );
        createLogFile( fsa, 100, 2, 2, 200,  99 );
        createLogFile( fsa, 1000, 3, 3, 2000,  999 );

        RecoveryProtocol protocol = new RecoveryProtocol( fsa, fileNames, readerPool, contentMarshal,
                NullLogProvider.getInstance() );

        // when
        State state = protocol.run();

        // then
        assertEquals( 2000, state.prevIndex );
        assertEquals( 999, state.prevTerm );

        assertEquals( -1, state.terms.get( 20 ) );
        assertEquals( -1, state.terms.get( 200 ) );
        assertEquals( -1, state.terms.get( 1999 ) );

        assertEquals( 999, state.terms.get( 2000 ) );
        assertEquals( -1, state.terms.get( 2001 ) );

        assertEquals( 999, state.terms.latest() );
    }

    private void createLogFile( EphemeralFileSystemAbstraction fsa, long prevFileLastIndex, long fileNameVersion,
            long headerVersion, long prevIndex, long prevTerm ) throws IOException
    {
        StoreChannel channel = fsa.open( fileNames.getForVersion( fileNameVersion ), OpenMode.READ_WRITE );
        PhysicalFlushableChannel writer = new PhysicalFlushableChannel( channel );
        headerMarshal.marshal( new SegmentHeader( prevFileLastIndex, headerVersion, prevIndex, prevTerm ), writer );
        writer.prepareForFlush().flush();
        channel.close();
    }

    private void createEmptyLogFile( EphemeralFileSystemAbstraction fsa, long fileNameVersion ) throws IOException
    {
        StoreChannel channel = fsa.open( fileNames.getForVersion( fileNameVersion ), OpenMode.READ_WRITE );
        channel.close();
    }
}
