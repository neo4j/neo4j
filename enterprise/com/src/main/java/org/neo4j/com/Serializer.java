package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jboss.netty.buffer.ChannelBuffer;

public interface Serializer
{
    void write( ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException;
}
