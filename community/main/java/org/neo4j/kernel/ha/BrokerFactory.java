package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.kernel.impl.ha.Broker;

public interface BrokerFactory
{
    Broker create( String storeDir, Map<String, String> graphDbConfig );
}
