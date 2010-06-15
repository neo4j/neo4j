package org.neo4j.kernel.management;

public interface LockManagerMBean
{
    final String NAME = "Locking";

    long getNumberOfAdvertedDeadlocks();
}
