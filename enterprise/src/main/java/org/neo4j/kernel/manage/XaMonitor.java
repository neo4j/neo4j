package org.neo4j.kernel.manage;

abstract class XaMonitor extends Neo4jJmx
{
    static class XaManager extends XaMonitor implements XaManagerMBean
    {
        XaManager( int instanceId )
        {
            super( instanceId );
        }
    }

    static class MXBeanImplementation extends XaMonitor implements
            XaManagerMXBean
    {
        MXBeanImplementation( int instanceId )
        {
            super( instanceId );
        }
    }

    private XaMonitor( int instanceId )
    {
        super( instanceId );
    }

    public XaResourceInfo[] getXaResources()
    {
        // TODO Auto-generated method stub
        return new XaResourceInfo[0];
    }
}
