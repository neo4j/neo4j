package org.neo4j.kernel.manage;

public interface LockManagerMBean
{
    final String NAME = "Locking";

    long getNumberOfAdvertedDeadlocks();
}
