package org.neo4j.management;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

class Main
{
    private static final String JCONSOLE = "sun.tools.jconsole.JConsole";

    private Main()
    {
    }

    public static void main( String[] args ) throws Throwable
    {
        Throwable failure = null;
        Class<?> jconsole = null;
        try
        {
            jconsole = Class.forName( JCONSOLE );
        }
        catch ( ClassNotFoundException ex )
        {
            ClassLoader loader;
            try
            {
                loader = new URLClassLoader(
                        new URL[] { new File( new File( System.getProperty( "java.home" ), "lib" ),
                                "jconsole.jar" ).toURI().toURL() }, Main.class.getClassLoader() );
            }
            catch ( MalformedURLException e )
            {
                System.err.println( "Could not find jconsole.jar" );
                return;
            }
            try
            {
                jconsole = loader.loadClass( JCONSOLE );
            }
            catch ( ClassNotFoundException e )
            {
                failure = ex;
            }
        }
        Method main = null;
        if ( jconsole != null )
        {
            try
            {
                main = jconsole.getMethod( "main", String[].class );
            }
            catch ( Exception ex )
            {
                failure = ex;
            }
        }
        if ( main != null )
        {
            try
            {
                main.invoke( null, new Object[] { args } );
                return;
            }
            catch ( InvocationTargetException e )
            {
                throw e.getTargetException();
            }
            catch ( Exception ex )
            {
                failure = ex;
            }
        }
        if ( failure != null )
        {
            System.err.println( "Failed to launch jconsole: " + failure );
            if ( !( failure instanceof ClassNotFoundException ) )
            {
                failure.printStackTrace();
            }
        }
    }
}
