package org.neo4j.kernel.manage;

public interface XaManagerMXBean
{
    final String NAME = "XA Resources";

    XaResourceInfo[] getXaResources();
}
