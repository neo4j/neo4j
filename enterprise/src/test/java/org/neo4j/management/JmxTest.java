package org.neo4j.management;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class JmxTest
{
    private static AbstractGraphDatabase graphDb;

    @BeforeClass
    public static synchronized void startGraphDb()
    {
        graphDb = new EmbeddedGraphDatabase( "target/var/" + JmxTest.class.getSimpleName() );
    }

    @BeforeClass
    public static synchronized void stopGraphDb()
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
            graphDb = null;
        }
    }

    @Test
    public void canAccessKernelBean() throws Exception
    {
        Kernel kernel = graphDb.getManagementBean( Kernel.class );
        assertNotNull( "kernel bean is null", kernel );
        assertNotNull( "MBeanQuery of kernel bean is null", kernel.getMBeanQuery() );
    }

    @Test
    public void canListAllBeans() throws Exception
    {
        Neo4jManager manager = getManager();
        assertTrue( "No beans returned", manager.allBeans().size() > 0 );
    }

    @Test
    public void canGetConfigurationParameters() throws Exception
    {
        Neo4jManager manager = getManager();
        Map<String, Object> configuration = manager.getConfiguration();
        assertTrue( "No configuration returned", configuration.size() > 0 );
    }

    private Neo4jManager getManager()
    {
        return new Neo4jManager( graphDb.getManagementBean( Kernel.class ) );
    }

    @Test
    public void canGetCacheBean() throws Exception
    {
        assertNotNull( getManager().getCacheBean() );
    }

    @Test
    public void canGetLockManagerBean() throws Exception
    {
        assertNotNull( getManager().getLockManagerBean() );
    }

    @Test
    public void canGetMemoryMappingBean() throws Exception
    {
        assertNotNull( getManager().getMemoryMappingBean() );
    }

    @Test
    public void canGetPrimitivesBean() throws Exception
    {
        assertNotNull( getManager().getPrimitivesBean() );
    }

    @Test
    public void canGetStoreFileBean() throws Exception
    {
        assertNotNull( getManager().getStoreFileBean() );
    }

    @Test
    public void canGetTransactionManagerBean() throws Exception
    {
        assertNotNull( getManager().getTransactionManagerBean() );
    }

    @Test
    public void canGetXaManagerBean() throws Exception
    {
        assertNotNull( getManager().getXaManagerBean() );
    }

    @Test
    public void canAccessMemoryMappingCompositData() throws Exception
    {
        assertNotNull( "MemoryPools is null", getManager().getMemoryMappingBean().getMemoryPools() );
    }

    @Test
    public void canAccessXaManagerCompositData() throws Exception
    {
        assertNotNull( "MemoryPools is null", getManager().getXaManagerBean().getXaResources() );
    }
}
