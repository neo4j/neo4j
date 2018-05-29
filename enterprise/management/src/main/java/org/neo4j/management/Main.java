/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
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
        // no instances
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
            File jar = new File( new File( System.getProperty( "java.home" ), "lib" ), "jconsole.jar" );
            if ( !jar.exists() )
            {
                jar = new File( new File( new File( System.getProperty( "java.home" ) ).getParentFile(), "lib" ),
                        "jconsole.jar" );
            }
            ClassLoader loader = null;
            if ( jar.exists() )
            {
                try
                {
                    loader = new URLClassLoader( new URL[] { jar.toURI().toURL() }, Main.class.getClassLoader() );
                }
                catch ( MalformedURLException e )
                {
                    // Handled by null check
                }
            }
            if ( loader == null )
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
                main = jconsole.getDeclaredMethod( "main", String[].class );
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
