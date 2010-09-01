package org.neo4j.kernel.ha;

public class BackupBroker extends FakeBroker
{
    public BackupBroker( Master master )
    {
        super( master );
    }

    @Override
    public void shutdown()
    {
        ((MasterClient) getMaster()).shutdown();
    }
}
