package org.neo4j.kernel.impl.management;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;

@Service.Implementation( KernelExtension.class )
public final class JmxExtension extends KernelExtension
{
    public JmxExtension()
    {
        super( "kernel jmx" );
    }

    @Override
    protected void load( final KernelData kernel )
    {
        final NodeManager nodeManager = kernel.getConfig().getGraphDbModule().getNodeManager();
        kernel.setState(
                this,
                Neo4jMBean.initMBeans( new Neo4jMBean.Creator(
                        kernel.instanceId(),
                        kernel.version(),
                        (NeoStoreXaDataSource) kernel.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                                "nioneodb" ) )
                {
                    @Override
                    protected void create( Neo4jMBean.Factory jmx )
                    {
                        jmx.createDynamicConfigurationMBean( kernel.getConfigParams() );
                        jmx.createPrimitiveMBean( nodeManager );
                        jmx.createStoreFileMBean();
                        jmx.createCacheMBean( nodeManager );
                        jmx.createLockManagerMBean( kernel.getConfig().getLockManager() );
                        jmx.createTransactionManagerMBean( kernel.getConfig().getTxModule() );
                        jmx.createMemoryMappingMBean( kernel.getConfig().getTxModule().getXaDataSourceManager() );
                        jmx.createXaManagerMBean( kernel.getConfig().getTxModule().getXaDataSourceManager() );
                    }
                } ) );
    }

    @Override
    protected void unload( KernelData kernel )
    {
        ( (Runnable) kernel.getState( this ) ).run();
    }

    public <T> T getBean( KernelData kernel, Class<T> beanClass )
    {
        if ( !isLoaded( kernel ) ) throw new IllegalStateException( "Not Loaded!" );
        return Neo4jMBean.getBean( kernel.instanceId(), beanClass );
    }
}
