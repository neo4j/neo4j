package org.neo4j.kernel.ha;

public class BackupBroker extends FakeSlaveBroker
{
    public BackupBroker( Master master, String storeDir )
    {
        super( master, -1, -1, storeDir );
    }

    @Override
    public void shutdown()
    {
        ((MasterClient) getMaster().first()).shutdown();
    }
}
