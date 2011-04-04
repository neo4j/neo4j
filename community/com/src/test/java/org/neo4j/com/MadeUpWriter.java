package org.neo4j.com;

import java.nio.channels.ReadableByteChannel;

public interface MadeUpWriter
{
    void write( ReadableByteChannel data );
}
