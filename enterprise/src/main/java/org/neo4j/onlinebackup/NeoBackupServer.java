package org.neo4j.onlinebackup;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class NeoBackupServer extends Thread
{
    private final int PORT;
    private volatile boolean run = true;

    NeoBackupServer( int port )
    {
        super();
        this.PORT = port;
    }

    @Override
    public void run()
    {
        ServerSocketChannel serverChannel = null;
        try
        {
            serverChannel = ServerSocketChannel.open();
            SocketAddress port = new InetSocketAddress( PORT );
            serverChannel.socket().bind( port );
            System.out.println( "Neo backup server bound to: " + PORT );
        }
        catch ( IOException e )
        {
            System.err.println( "Failed binding server socket on Neo backup "
                + "server, " + e.getMessage() );
            e.printStackTrace();
            System.err
                .println( "NeoBackupServer thread on " + PORT + " killed" );
            return;
        }
        while ( run )
        {
            try
            {
                SocketChannel clientChannel = serverChannel.accept();
                String clientAddress = 
                    clientChannel.socket().getInetAddress().toString();
                int clientPort = clientChannel.socket().getPort();
                System.out.println( "Accepted client: " + clientAddress + 
                    ":" + clientPort );
                String response = "Hello " + clientAddress + " on port "
                    + clientPort + "\r\n";
                response += "This is " + serverChannel.socket() + " on port "
                    + serverChannel.socket().getLocalPort() + "\r\n";
                byte[] data = response.getBytes( "UTF-8" );
                ByteBuffer buffer = ByteBuffer.wrap( data );
                while ( buffer.hasRemaining() )
                    clientChannel.write( buffer );
                clientChannel.close();
            }
            catch ( IOException e )
            {
                System.err.println( "Failed to accept client, " + 
                    e.getMessage() );
                e.printStackTrace();
            }
        }
        try
        {
            serverChannel.close();
        }
        catch ( IOException e )
        {
            System.err.println( "Failed closing server socket on Neo backup "
                + "server, " + e.getMessage() );
            e.printStackTrace();
            System.err
                .println( "NeoBackupServer thread on " + PORT + " killed" );
        }
    }

    public void shutdown()
    {
        run = false;
    }
    
    public static void main( String args[] )
    {
        NeoBackupServer nbs = new NeoBackupServer( 5678 );
        nbs.start();
        try
        {
            while ( true )
            {
                try
                {
                    Thread.sleep( 3000 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
            }
        }
        finally
        {
            nbs.shutdown();
        }
    }
}
