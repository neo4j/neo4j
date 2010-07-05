package org.neo4j.kernel.management;

import java.io.Serializable;

public final class XaResourceInfo implements Serializable
{
    private final String name;
    private final String branchId;

    /* Java 1.6 specific
    @ConstructorProperties( { "name", "branchId" } )
    */
    public XaResourceInfo( String name, String branchId )
    {
        this.name = name;
        this.branchId = branchId;
    }

    public String getName()
    {
        return name;
    }

    public String getBranchId()
    {
        return branchId;
    }
}
