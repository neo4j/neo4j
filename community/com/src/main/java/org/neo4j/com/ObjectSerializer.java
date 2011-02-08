package org.neo4j.com;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

public interface ObjectSerializer<T>
{
    void write( T responseObject, ChannelBuffer result ) throws IOException;
}
