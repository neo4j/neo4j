package org.neo4j.kernel.ha;

import org.neo4j.kernel.impl.ha.Master;

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
