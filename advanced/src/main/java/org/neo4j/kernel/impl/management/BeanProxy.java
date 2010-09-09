package org.neo4j.kernel.impl.management;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.management.MBeanServer;
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

    static <T> T load( MBeanServer mbs, Class<T> beanInterface, ObjectName name )
    {
        return factory.makeProxy( mbs, beanInterface, name );
    }

    static boolean supportsMxBeans()
    {
        return factory != null && factory.supportsMxBeans;
    }

    abstract <T> T makeProxy( MBeanServer mbs, Class<T> beanInterface, ObjectName name );

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
        private final Method isMXBeanInterface;
        private final Method newMBeanProxy;
        private final Method newMXBeanProxy;

        Java6ProxyMaker() throws Exception
        {
            super( true );
            Class<?> JMX = Class.forName( "javax.management.JMX" );
            this.isMXBeanInterface = JMX.getMethod( "isMXBeanInterface", Class.class );
            this.newMBeanProxy = JMX.getMethod( "newMBeanProxy", MBeanServerConnection.class,
                    ObjectName.class, Class.class );
            this.newMXBeanProxy = JMX.getMethod( "newMXBeanProxy", MBeanServerConnection.class,
                    ObjectName.class, Class.class );
        }

        @Override
        <T> T makeProxy( MBeanServer mbs, Class<T> beanType, ObjectName name )
        {
            try
            {
                final Method factoryMethod;
                if ( isMXBeanInterface( beanType ) )
                {
                    factoryMethod = newMXBeanProxy;
                }
                else
                {
                    factoryMethod = newMBeanProxy;
                }
                return beanType.cast( factoryMethod.invoke( null, mbs, name, beanType ) );
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

        private boolean isMXBeanInterface( Class<?> interfaceClass ) throws Exception
        {
            return (Boolean) isMXBeanInterface.invoke( null, interfaceClass );
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
        <T> T makeProxy( MBeanServer mbs, Class<T> beanType, ObjectName name )
        {
            return MBeanServerInvocationHandler.newProxyInstance( mbs, name, beanType, false );
        }
    }
}
