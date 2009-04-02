package org.neo4j.onlinebackup;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Wrap a XA data source.
 */
public interface Resource
{
    long getCreationTime();

    long getIdentifier();

    String getName();

    long getVersion();

    boolean hasLogicalLog( long version );

    ReadableByteChannel getLogicalLog( long version ) throws IOException;

    void applyLog( ReadableByteChannel log ) throws IOException;

    void rotateLog() throws IOException;

    void makeBackupSlave();

    void close();
}
