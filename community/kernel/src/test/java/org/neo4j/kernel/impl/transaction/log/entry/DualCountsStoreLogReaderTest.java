package org.neo4j.kernel.impl.transaction.log.entry;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.store.counts.CountsSnapshot;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.store.counts.CountsSnapshot.NO_SNAPSHOT;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;

/**
 * While the new counts store is in limbo, it we'll have both implementations of the counts stores in the code base
 * working side-by-side. Once they new one works, and is confirmed to be correct, we can remove the old one. Once
 * that happens we could probabaly remove this test.
 */
public class DualCountsStoreLogReaderTest
{
    private final VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader =
            new VersionAwareLogEntryReader<>( new RecordStorageCommandReaderFactory() );

    @Test
    public void shouldBeSnapshotLogType() throws IOException
    {
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel();
        LogEntryWriter logEntryWriter = new LogEntryWriter( logChannel );
        LogPosition logPosition = new LogPosition( 1, 0 );
        CountsSnapshot countsSnapshot = new CountsSnapshot( 1, new ConcurrentHashMap<CountsKey,long[]>()
        {{
            put( nodeKey( 1 ), new long[]{42} );
            put( nodeKey( 2 ), new long[]{42} );
        }} );

        // when
        logEntryWriter.writeCheckPointEntry( logPosition, countsSnapshot );
        LogEntry logEntry = logEntryReader.readLogEntry( logChannel );

        // then
        assertEquals( LogEntryByteCodes.CHECK_POINT_SNAPSHOT, logEntry.getType() );
    }

    @Test
    public void shouldBeCheckpointLogType() throws IOException
    {
        InMemoryClosableChannel logChannel = new InMemoryClosableChannel();
        LogEntryWriter logEntryWriter = new LogEntryWriter( logChannel );
        LogPosition logPosition = new LogPosition( 1, 0 );

        // when
        logEntryWriter.writeCheckPointEntry( logPosition, NO_SNAPSHOT );
        LogEntry logEntry = logEntryReader.readLogEntry( logChannel );

        // then
        assertEquals( LogEntryByteCodes.CHECK_POINT, logEntry.getType() );
    }
}

