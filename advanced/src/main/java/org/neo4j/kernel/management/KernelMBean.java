package org.neo4j.kernel.management;

import java.util.Date;

public interface KernelMBean
{
    final String NAME = "Kernel";

    String getStoreDirectory();

    String getKernelVersion();

    Date getKernelStartTime();

    Date getStoreCreationDate();

    String getStoreId();

    long getStoreLogVersion();

    boolean isReadOnly();
}
