package org.neo4j.impl.util;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

public class UdpPinger
{
    private final ByteBuffer buf;
    private final SocketAddress host;
    
    public UdpPinger( ByteBuffer buf, SocketAddress host )
    {
        this.buf = buf;
        this.host = host;
    }
    
    public void sendPing()
    {
        try
        {
            DatagramChannel sendChannel = DatagramChannel.open();
            // blocking but will still just throw it at OS since it is
            // UDP (will not block, just fail)
            // one time try, ignore 0 bytes sent
            sendChannel.send( buf, host );
            sendChannel.close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            // ok we tried
        }
    }
}