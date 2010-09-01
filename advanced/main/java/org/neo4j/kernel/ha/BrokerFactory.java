package org.neo4j.kernel.ha;

import java.util.Map;

public interface BrokerFactory
{
    Broker create( String storeDir, Map<String, String> graphDbConfig );
}
