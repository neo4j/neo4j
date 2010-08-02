package org.neo4j.kernel.management;

import java.util.Date;

import javax.management.ObjectName;

public interface Kernel
{
    final String NAME = "Kernel";

    ObjectName getMBeanQuery();

    String getStoreDirectory();

    String getKernelVersion();

    Date getKernelStartTime();

    Date getStoreCreationDate();

    String getStoreId();

    long getStoreLogVersion();

    boolean isReadOnly();
}
