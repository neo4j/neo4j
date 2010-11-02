package org.neo4j.management.impl;

import java.util.ArrayList;
import java.util.List;

import javax.management.NotCompliantMBeanException;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension.KernelData;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.management.XaManager;
import org.neo4j.management.XaResourceInfo;

@Service.Implementation( ManagementBeanProvider.class )
public final class XaManagerBean extends ManagementBeanProvider
{
    public XaManagerBean()
    {
        super( XaManager.class );
    }

    @Override
    protected Neo4jMBean createMBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new XaManagerImpl( null, kernel );
    }

    @Override
    protected Neo4jMBean createMXBean( KernelData kernel ) throws NotCompliantMBeanException
    {
        return new XaManagerImpl( this, kernel, true );
    }

    @Description( "Information about the XA transaction manager" )
    private static class XaManagerImpl extends Neo4jMBean implements XaManager
    {
        private final XaDataSourceManager datasourceMananger;

        XaManagerImpl( ManagementBeanProvider provider, KernelData kernel )
                throws NotCompliantMBeanException
        {
            super( provider, kernel );
            this.datasourceMananger = kernel.getConfig().getTxModule().getXaDataSourceManager();
        }

        XaManagerImpl( XaManagerBean provider, KernelData kernel, boolean isMxBean )
        {
            super( provider, kernel, isMxBean );
            this.datasourceMananger = kernel.getConfig().getTxModule().getXaDataSourceManager();
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
}
