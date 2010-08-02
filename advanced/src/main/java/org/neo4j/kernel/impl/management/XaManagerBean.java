package org.neo4j.kernel.impl.management;

import java.util.ArrayList;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.management.XaManager;
import org.neo4j.kernel.management.XaResourceInfo;

@Description( "Information about the XA transaction manager" )
class XaManagerBean extends Neo4jMBean implements XaManager
{
    static XaManagerBean create( final int instanceId, final XaDataSourceManager datasourceMananger )
    {
        return createMX( new MXFactory<XaManagerBean>()
        {
            @Override
            XaManagerBean createMXBean()
            {
                return new XaManagerBean( instanceId, datasourceMananger, true );
            }

            @Override
            XaManagerBean createStandardMBean() throws NotCompliantMBeanException
            {
                return new XaManagerBean( instanceId, datasourceMananger );
            }
        } );
    }

    private final XaDataSourceManager datasourceMananger;

    private XaManagerBean( int instanceId, XaDataSourceManager datasourceMananger )
            throws NotCompliantMBeanException
    {
        super( instanceId, XaManager.class );
        this.datasourceMananger = datasourceMananger;
    }

    private XaManagerBean( int instanceId, XaDataSourceManager datasourceMananger, boolean isMXBean )
    {
        super( instanceId, XaManager.class, isMXBean );
        this.datasourceMananger = datasourceMananger;
    }

    @Description( "Information about all XA resources managed by the transaction manager" )
    public XaResourceInfo[] getXaResources()
    {
        return getXaResourcesImpl( datasourceMananger );
    }

    private static XaResourceInfo[] getXaResourcesImpl( XaDataSourceManager datasourceMananger )
    {
        List<XaResourceInfo> result = new ArrayList<XaResourceInfo>();
        for ( XaDataSource datasource : datasourceMananger.getAllRegisteredDataSources() )
        {
            result.add( createXaResourceInfo( datasource ) );
        }
        return result.toArray( new XaResourceInfo[result.size()] );
    }

    private static XaResourceInfo createXaResourceInfo( XaDataSource datasource )
    {
        return new XaResourceInfo( datasource.getName(), toHexString( datasource.getBranchId() ) );
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
