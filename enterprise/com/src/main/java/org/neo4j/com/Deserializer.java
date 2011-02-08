package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

public interface Deserializer<T>
{
    T read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException;
}
