package org.neo4j.kernel.management;

public interface LockManager
{
    final String NAME = "Locking";

    long getNumberOfAdvertedDeadlocks();
}
