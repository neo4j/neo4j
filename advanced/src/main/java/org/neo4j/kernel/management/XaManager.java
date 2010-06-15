package org.neo4j.kernel.management;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

class XaManager extends Neo4jJmx implements XaManagerMBean
{
    private final XaDataSourceManager datasourceMananger;

    static class AsMXBean extends Neo4jJmx implements XaManagerMXBean
    {
        private final XaDataSourceManager datasourceMananger;

        AsMXBean( int instanceId, XaDataSourceManager datasourceMananger )
        {
            super( instanceId );
            this.datasourceMananger = datasourceMananger;
        }

        public XaResourceInfo[] getXaResources()
        {
            return getXaResourcesImpl( datasourceMananger );
        }
    }

    XaManager( int instanceId, XaDataSourceManager datasourceMananger )
    {
        super( instanceId );
        this.datasourceMananger = datasourceMananger;
    }

    public XaResourceInfo[] getXaResources()
    {
        return getXaResourcesImpl( datasourceMananger );
    }

    private static XaResourceInfo[] getXaResourcesImpl(
            XaDataSourceManager datasourceMananger )
    {
        List<XaResourceInfo> result = new ArrayList<XaResourceInfo>();
        for ( XaDataSource datasource : datasourceMananger.getAllRegisteredDataSources() )
        {
            result.add( new XaResourceInfo( datasource ) );
        }
        return result.toArray( new XaResourceInfo[result.size()] );
    }
}
