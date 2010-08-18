package org.neo4j.kernel;

class UpdatePuller implements Runnable
{
    private final HighlyAvailableGraphDatabase db;
    private final long intervalMillis;
    private volatile boolean halted;

    private UpdatePuller( HighlyAvailableGraphDatabase db, long intervalMillis )
    {
        this.db = db;
        this.intervalMillis = intervalMillis;
    }
    
    public static UpdatePuller startAutoPull( HighlyAvailableGraphDatabase db,
            long intervalMillis )
    {
        UpdatePuller puller = new UpdatePuller( db, intervalMillis );
        Thread thread = new Thread( puller, "Pullupdates thread" );
        thread.start();
        return puller;
    }
    
    public synchronized void halt()
    {
        this.halted = true;
    }
    
    public void run()
    {
        while ( !halted )
        {
            waitWithRegardsToHalt();
            if ( !halted )
            {
                db.pullUpdates();
            }
        }
    }

    private void waitWithRegardsToHalt()
    {
        long t = System.currentTimeMillis();
        while ( System.currentTimeMillis()-t < intervalMillis )
        {
            try
            {
                Thread.sleep( Math.min( 100, System.currentTimeMillis()-t ) );
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
                continue;
            }
        }
    }
}
