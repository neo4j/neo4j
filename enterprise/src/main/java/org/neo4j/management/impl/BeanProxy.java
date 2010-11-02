package org.neo4j.management.impl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

abstract class BeanProxy
{
    final boolean supportsMxBeans;

    private BeanProxy( boolean supportsMxBeans )
    {
        this.supportsMxBeans = supportsMxBeans;
    }

    static <T> T load( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName name )
    {
        return factory.makeProxy( mbs, beanInterface, name );
    }

    static boolean supportsMxBeans()
    {
        return factory != null && factory.supportsMxBeans;
    }

    abstract <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanInterface, ObjectName name );

    private static final BeanProxy factory;
    static
    {
        BeanProxy proxyMaker;
        try
        {
            proxyMaker = new Java6ProxyMaker();
        }
        catch ( Exception t )
        {
            proxyMaker = null;
        }
        catch ( LinkageError t )
        {
            proxyMaker = null;
        }
        if ( proxyMaker == null )
        {
            try
            {
                proxyMaker = new Java5ProxyMaker();
            }
            catch ( Exception t )
            {
                proxyMaker = null;
            }
            catch ( LinkageError t )
            {
                proxyMaker = null;
            }
        }
        factory = proxyMaker;
    }

    private static class Java6ProxyMaker extends BeanProxy
    {
        private final Method newMXBeanProxy;

        Java6ProxyMaker() throws Exception
        {
            super( true );
            Class<?> JMX = Class.forName( "javax.management.JMX" );
            this.newMXBeanProxy = JMX.getMethod( "newMXBeanProxy", MBeanServerConnection.class,
                    ObjectName.class, Class.class );
        }

        @Override
        <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanType, ObjectName name )
        {
            try
            {
                return beanType.cast( newMXBeanProxy.invoke( null, mbs, name, beanType ) );
            }
            catch ( InvocationTargetException exception )
            {
                throw launderRuntimeException( exception.getTargetException() );
            }
            catch ( Exception exception )
            {
                throw new UnsupportedOperationException(
                        "Creating Management Bean proxies requires Java 1.6", exception );
            }
        }

        static RuntimeException launderRuntimeException( Throwable exception )
        {
            if ( exception instanceof RuntimeException )
            {
                return (RuntimeException) exception;
            }
            else if ( exception instanceof Error )
            {
                throw (Error) exception;
            }
            else
            {
                throw new RuntimeException( "Unexpected Exception!", exception );
            }
        }
    }

    private static class Java5ProxyMaker extends BeanProxy
    {
        Java5ProxyMaker() throws Exception
        {
            super( false );
            Class.forName( "javax.management.MBeanServerInvocationHandler" );
        }

        @Override
        <T> T makeProxy( MBeanServerConnection mbs, Class<T> beanType, ObjectName name )
        {
            return MBeanServerInvocationHandler.newProxyInstance( mbs, name, beanType, false );
        }
    }
}
