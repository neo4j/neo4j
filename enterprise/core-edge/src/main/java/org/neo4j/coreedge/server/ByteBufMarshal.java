package org.neo4j.coreedge.server;

import io.netty.buffer.ByteBuf;

public interface ByteBufMarshal<TARGET>
{
    void marshal( TARGET member, ByteBuf buffer );

    TARGET unmarshal( ByteBuf buffer );
}
