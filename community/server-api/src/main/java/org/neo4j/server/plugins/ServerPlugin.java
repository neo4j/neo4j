/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.plugins;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.helpers.Service;

/**
 * API for creating extensions for the Neo4j server.
 * <p>
 * Extensions are created by creating a subclass of this class. The subclass
 * should have a public no-argument constructor (or no constructor at all).
 * Then place this class in a jar-file that contains a file called
 * <code>META-INF/services/org.neo4j.server.plugins.ServerPlugin</code>.
 * This file should contain the fully qualified name of the class that extends
 * {@link ServerPlugin}, e.g. <code>com.example.MyNeo4jServerExtension</code>
 * on a single line. If the jar contains multiple extensions to the Neo4j
 * server, this file should contain the fully qualified class names of all
 * extension classes, each class name on its own line in the file. When the jar
 * file is placed on the class path of the server, it will be loaded
 * automatically when the server starts.
 * <p>
 * The easiest way to implement Neo4j server extensions is by defining public
 * methods on the extension class annotated with
 * <code>@{@link PluginTarget}</code>. The parameter for the
 * {@link PluginTarget} annotation should be the class representing the
 * entity that the method extends. The entities that can be extended are
 * currently:
 * <ul>
 * <li>{@link org.neo4j.graphdb.GraphDatabaseService} - a general extension
 * method to the server</li>
 * <li>{@link org.neo4j.graphdb.Node} - an extension method for a node</li>
 * <li>{@link org.neo4j.graphdb.Relationship} - an extension method for a
 * relationship</li>
 * </ul>
 * You can then use the <code>@{@link Source}</code> annotation on one parameter of the method to get
 * the instance that was extended. Additional parameters needs to be of a
 * supported type, and annotated with the <code>@{@link Parameter}</code> annotation specifying the name by which the
 * parameter is passed from the client. The supported parameter types are:
 * <ul>
 * <li>{@link org.neo4j.graphdb.Node}</li>
 * <li>{@link org.neo4j.graphdb.Relationship}</li>
 * <li>{@link java.net.URI} or {@link java.net.URL}</li>
 * <li>{@link Integer}</li>
 * <li>{@link Long}</li>
 * <li>{@link Short}</li>
 * <li>{@link Byte}</li>
 * <li>{@link Character}</li>
 * <li>{@link Boolean}</li>
 * <li>{@link Double}</li>
 * <li>{@link Float}</li>
 * <li>{@link java.util.List lists}, {@link java.util.Set sets} or arrays of any
 * of the above types.</li>
 * </ul>
 * <p>
 * All exceptions thrown by an {@link PluginTarget} method are treated as
 * server errors, unless the method has declared the ability to throw such an
 * exception, in that case the exception is treated as a bad request and
 * propagated to the invoking client.
 *
 * @see java.util.ServiceLoader
 */
public abstract class ServerPlugin
{
    final String name;

    /**
     * Create a server extension with the specified name.
     *
     * @param name the name of this extension.
     */
    public ServerPlugin( String name )
    {
        this.name = verifyName( name );
    }

    /**
     * Create a server extension using the simple name of the concrete class
     * that extends {@link ServerPlugin} as the name for the extension.
     */
    public ServerPlugin()
    {
        this.name = verifyName( getClass().getSimpleName() );
    }

    static String verifyName( String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "Name may not be null" );
        }
        try
        {
            if ( !URLEncoder.encode( name, "UTF-8" ).equals( name ) )
            {
                throw new IllegalArgumentException( "Name contains illegal characters" );
            }
        } catch ( UnsupportedEncodingException e )
        {
            throw new Error( "UTF-8 should be supported", e );
        }
        return name;
    }

    @Override
    public String toString()
    {
        return "ServerPlugin[" + name + "]";
    }

    static Iterable<ServerPlugin> load()
    {
        return Service.load( ServerPlugin.class );
    }

    /**
     * Loads the extension points of this server extension. Override this method
     * to provide your own, custom way of loading extension points. The default
     * implementation loads {@link PluginPoint} based on methods with the
     * {@link PluginTarget} annotation.
     *
     * @param extender     the collection of {@link org.neo4j.server.plugins.PluginPoint}s for this
     *                     {@link org.neo4j.server.plugins.ServerPlugin}.
     *
     */
    protected void loadServerExtender( ServerExtender extender )
    {
        for ( PluginPoint plugin : getDefaultExtensionPoints( extender.getPluginPointFactory() ) )
        {
            extender.addExtension( plugin.forType(), plugin );
        }
    }

    /**
     * Loads the extension points of this server extension. Override this method
     * to provide your own, custom way of loading extension points. The default
     * implementation loads {@link PluginPoint} based on methods with the
     * {@link PluginTarget} annotation.
     *
     * @return the collection of {@link PluginPoint}s for this
     *         {@link ServerPlugin}.
     */
    protected Collection<PluginPoint> getDefaultExtensionPoints( PluginPointFactory pluginPointFactory )
    {
        List<PluginPoint> result = new ArrayList<PluginPoint>();
        for ( Method method : getClass().getMethods() )
        {
            PluginTarget target = method.getAnnotation( PluginTarget.class );
            if ( target != null )
            {
                result.add( pluginPointFactory.createFrom( this, method, target.value() ) );
            }
        }
        return result;
    }
}
