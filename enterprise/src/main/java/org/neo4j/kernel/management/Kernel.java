package org.neo4j.kernel.management;

import java.util.Date;

public interface Kernel
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
