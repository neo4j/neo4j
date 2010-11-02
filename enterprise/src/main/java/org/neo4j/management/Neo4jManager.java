package org.neo4j.management;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.neo4j.management.impl.ConfigurationBean;
import org.neo4j.management.impl.KernelProxy;

public final class Neo4jManager extends KernelProxy implements Kernel
{
    public static Neo4jManager get()
    {
        return get( getPlatformMBeanServer() );
    }

    public static Neo4jManager get( String kernelIdentifier )
    {
        return get( getPlatformMBeanServer(), kernelIdentifier );
    }

    public static Neo4jManager get( MBeanServerConnection server )
    {
        server.getClass();
        try
        {
            return get( server, server.queryNames( getObjectName( Kernel.class, null ), null ) );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Connection failed.", e );
        }
    }

    public static Neo4jManager get( MBeanServerConnection server, String kernelIdentifier )
    {
        server.getClass();
        kernelIdentifier.getClass();
        try
        {
            return get( server, server.queryNames( getObjectName( Kernel.class, kernelIdentifier ),
                    null ) );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Connection failed.", e );
        }
    }

    private static Neo4jManager get( MBeanServerConnection server, Collection<ObjectName> kernels )
    {
        if ( kernels.size() == 0 )
        {
            throw new NoSuchElementException( "No matching Neo4j Graph Database running on server" );
        }
        else if ( kernels.size() == 1 )
        {
            return new Neo4jManager( server,
                    proxy( server, Kernel.class, kernels.iterator().next() ) );
        }
        else
        {
            throw new NoSuchElementException(
                    "Too many matching Neo4j Graph Databases running on server" );
        }
    }

    private final ObjectName config;

    public Neo4jManager( Kernel kernel )
    {
        this( getServer( kernel ), kernel );
    }

    private static MBeanServerConnection getServer( Kernel kernel )
    {
        if ( kernel instanceof Proxy )
        {
            InvocationHandler handler = Proxy.getInvocationHandler( kernel );
            if ( handler instanceof MBeanServerInvocationHandler )
            {
                return ( (MBeanServerInvocationHandler) handler ).getMBeanServerConnection();

            }
        }
        throw new UnsupportedOperationException( "Cannot get server for kernel: " + kernel );
    }

    private Neo4jManager( MBeanServerConnection server, Kernel kernel )
    {
        super( server, kernel );
        this.config = getObjectName( ConfigurationBean.CONFIGURATION_MBEAN_NAME );
    }

    public Cache getCacheBean()
    {
        return getBean( Cache.class );
    }

    public LockManager getLockManagerBean()
    {
        return getBean( LockManager.class );
    }

    public MemoryMapping getMemoryMappingBean()
    {
        return getBean( MemoryMapping.class );
    }

    public Primitives getPrimitivesBean()
    {
        return getBean( Primitives.class );
    }

    public StoreFile getStoreFileBean()
    {
        return getBean( StoreFile.class );
    }

    public TransactionManager getTransactionManagerBean()
    {
        return getBean( TransactionManager.class );
    }

    public XaManager getXaManagerBean()
    {
        return getBean( XaManager.class );
    }

    public Object getConfigurationParameter( String key )
    {
        try
        {
            return server.getAttribute( config, key );
        }
        catch ( AttributeNotFoundException e )
        {
            return null;
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not access the configuration bean", e );
        }
    }

    public Map<String, Object> getConfiguration()
    {
        final String[] keys;
        final AttributeList attributes;
        try
        {
            MBeanAttributeInfo[] keyInfo = server.getMBeanInfo( config ).getAttributes();
            keys = new String[keyInfo.length];
            for ( int i = 0; i < keys.length; i++ )
            {
                keys[i] = keyInfo[i].getName();
            }
            attributes = server.getAttributes( config, keys );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Could not access the configuration bean", e );
        }
        Map<String, Object> configuration = new HashMap<String, Object>();
        for ( int i = 0; i < keys.length; i++ )
        {
            configuration.put( keys[i], attributes.get( i ) );

        }
        return configuration;
    }

    @Override
    public Date getKernelStartTime()
    {
        return kernel.getKernelStartTime();
    }

    @Override
    public String getKernelVersion()
    {
        return kernel.getKernelVersion();
    }

    @Override
    public ObjectName getMBeanQuery()
    {
        return kernel.getMBeanQuery();
    }

    @Override
    public Date getStoreCreationDate()
    {
        return kernel.getStoreCreationDate();
    }

    @Override
    public String getStoreDirectory()
    {
        return kernel.getStoreDirectory();
    }

    @Override
    public String getStoreId()
    {
        return kernel.getStoreId();
    }

    @Override
    public long getStoreLogVersion()
    {
        return kernel.getStoreLogVersion();
    }

    @Override
    public boolean isReadOnly()
    {
        return kernel.isReadOnly();
    }
}
