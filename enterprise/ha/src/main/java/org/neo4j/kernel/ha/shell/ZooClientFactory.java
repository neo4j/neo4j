package org.neo4j.kernel.ha.shell;

import org.neo4j.kernel.ha.zookeeper.ZooClient;

public interface ZooClientFactory
{
    ZooClient newZooClient();
}
