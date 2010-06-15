package org.neo4j.kernel.management;

public interface XaManagerMXBean
{
    final String NAME = "XA Resources";

    XaResourceInfo[] getXaResources();
}
