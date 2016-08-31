package org.neo4j.coreedge.messaging.address;

import java.io.IOException;

import org.neo4j.coreedge.core.state.storage.SafeChannelMarshal;
import org.neo4j.coreedge.messaging.EndOfStreamException;
import org.neo4j.coreedge.messaging.marshalling.StringMarshal;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class AdvertisedSocketAddressChannelMarshal extends SafeChannelMarshal<AdvertisedSocketAddress>
{
    @Override
    public void marshal( AdvertisedSocketAddress address, WritableChannel channel ) throws IOException
    {
        StringMarshal.marshal( channel, address.toString() );
    }

    @Override
    public AdvertisedSocketAddress unmarshal0( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        String host = StringMarshal.unmarshal( channel );
        return new AdvertisedSocketAddress( host );
    }
}
