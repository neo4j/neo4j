package org.neo4j.kernel.manage;

import java.beans.ConstructorProperties;
import java.io.Serializable;

public final class XaResourceInfo implements Serializable
{
    private final String name;

    @ConstructorProperties( { "Name" } )
    public XaResourceInfo( String name )
    {
        this.name = name;
    }

    public String getName()
    {
        return name;
    }
}
