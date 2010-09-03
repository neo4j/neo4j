package org.neo4j.kernel.ha;

public class BackupBroker extends FakeSlaveBroker
{
    public BackupBroker( Master master )
    {
        super( master, -1, -1 );
    }

    @Override
    public void shutdown()
    {
        ((MasterClient) getMaster()).shutdown();
    }
}
