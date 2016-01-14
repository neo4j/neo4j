package org.neo4j.coreedge.raft.state;

import java.io.IOException;

import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public interface ChannelMarshal<STATE>
{
    void marshal( STATE target, WritableChannel channel ) throws IOException;

    STATE unmarshal( ReadableChannel source ) throws IOException;
}
