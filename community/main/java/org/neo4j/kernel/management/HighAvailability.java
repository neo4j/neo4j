package org.neo4j.kernel.management;

public interface HighAvailability
{
    final String NAME = "High Availability";

    String getMachineId();

    boolean isMaster();

    SlaveInfo[] getConnectedSlaves();

    String update();
}
