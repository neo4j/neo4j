package org.neo4j.kernel.ha;

import org.neo4j.kernel.impl.ha.Broker;

public interface BrokerFactory
{
    Broker create();
}
