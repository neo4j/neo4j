package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

public interface MasterCaller<M, R>
{
    Response<R> callMaster( M master, SlaveContext context, ChannelBuffer input, ChannelBuffer target );
}
