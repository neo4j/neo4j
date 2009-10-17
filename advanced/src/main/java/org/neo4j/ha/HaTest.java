package org.neo4j.ha;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Transaction;

public class HaTest
{
    public static void main( String args[] ) throws Exception
    {
        Map<String,String> params = new HashMap<String,String>();
        Master master = new Master( "var/master", params, 9050 );
        ReadOnlySlave slave = new ReadOnlySlave( "var/slave", params, 
            "127.0.0.1", 9050 );
        Thread.sleep( 2000 );
        NeoService neo = master.getNeoService();
        Transaction tx = neo.beginTx();
        long nodeId = -1;
        try
        {
            nodeId = neo.createNode().getId();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        neo = slave.getNeoService();
        tx = neo.beginTx();
        try
        {
            neo.getNodeById( nodeId );
            throw new RuntimeException( "Node should not exist" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
        finally
        {
            tx.finish();
        }
        master.rotateLogAndPushToSlaves();
        Thread.sleep( 5000 );
        tx = neo.beginTx();
        try
        {
            neo.getNodeById( nodeId );
        }
        finally
        {
            tx.finish();
            master.shutdown();
            slave.shutdown();
        }
    }
}