package org.neo4j.kernel.manage;

import java.beans.ConstructorProperties;
import java.io.Serializable;

import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public final class XaResourceInfo implements Serializable
{
    private final String name;
    private final String branchId;

    @ConstructorProperties( { "name", "branchId" } )
    public XaResourceInfo( String name, String branchId )
    {
        this.name = name;
        this.branchId = branchId;
    }

    XaResourceInfo( XaDataSource datasource )
    {
        this( datasource.getName(), toHexString( datasource.getBranchId() ) );
    }

    public String getName()
    {
        return name;
    }

    public String getBranchId()
    {
        return branchId;
    }

    private static String toHexString( byte[] branchId )
    {
        StringBuilder result = new StringBuilder();
        for ( byte part : branchId )
        {
            String chunk = Integer.toHexString( part );
            if ( chunk.length() < 2 ) result.append( "0" );
            if ( chunk.length() > 2 )
                result.append( chunk.substring( chunk.length() - 2 ) );
            else
                result.append( chunk );
        }
        return result.toString();
    }
}
