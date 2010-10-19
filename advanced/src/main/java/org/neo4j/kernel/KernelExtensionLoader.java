package org.neo4j.kernel;

import java.util.Map;

public interface KernelExtensionLoader
{
    void load( Map<Object, Object> params );
}
