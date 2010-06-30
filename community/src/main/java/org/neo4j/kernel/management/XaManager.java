package org.neo4j.kernel.management;

public interface XaManager
{
    final String NAME = "XA Resources";

    XaResourceInfo[] getXaResources();
}
