package org.neo4j.kernel.management;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

abstract class XaMonitor extends Neo4jJmx
{
    static class XaManager extends XaMonitor implements XaManagerMBean
    {
        XaManager( int instanceId, XaDataSourceManager datasourceMananger )
        {
            super( instanceId, datasourceMananger );
        }
    }

    static class MXBeanImplementation extends XaMonitor implements
            XaManagerMXBean
    {
        MXBeanImplementation( int instanceId, XaDataSourceManager datasourceMananger )
        {
            super( instanceId, datasourceMananger );
        }
    }

    private final XaDataSourceManager datasourceMananger;

    private XaMonitor( int instanceId, XaDataSourceManager datasourceMananger )
    {
        super( instanceId );
        this.datasourceMananger = datasourceMananger;
    }

    public XaResourceInfo[] getXaResources()
    {
        List<XaResourceInfo> result = new ArrayList<XaResourceInfo>();
        for ( XaDataSource datasource : datasourceMananger.getAllRegisteredDataSources() )
        {
            result.add( new XaResourceInfo( datasource ) );
        }
        return result.toArray( new XaResourceInfo[result.size()] );
    }
}
