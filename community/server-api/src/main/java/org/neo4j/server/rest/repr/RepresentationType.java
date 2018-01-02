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
package org.neo4j.server.rest.repr;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;

public final class RepresentationType
{
    private static final Map<Class<?>, Class<?>> boxed = new HashMap<>();

    static
    {
        boxed.put( byte.class, Byte.class );
        boxed.put( char.class, Character.class );
        boxed.put( short.class, Short.class );
        boxed.put( int.class, Integer.class );
        boxed.put( long.class, Long.class );
        boxed.put( float.class, Float.class );
        boxed.put( double.class, Double.class );
        boxed.put( boolean.class, Boolean.class );
    }

    private static final Map<String, RepresentationType> types = new HashMap<>();
    private static final Map<Class<?>, RepresentationType> extended = new HashMap<>();
    // Graph database types
    public static final RepresentationType
            GRAPHDB = new RepresentationType( "graphdb", null, GraphDatabaseService.class ),
            NODE = new RepresentationType( "node", "nodes", Node.class ),
            RELATIONSHIP = new RepresentationType( "relationship", "relationships", Relationship.class ),
            PATH = new RepresentationType( "path", "paths", Path.class ),
            FULL_PATH = new RepresentationType( "full-path", "full-paths", FullPath.class),
            RELATIONSHIP_TYPE = new RepresentationType( "relationship-type", "relationship-types", RelationshipType.class ),
            PROPERTIES = new RepresentationType( "properties" ),
            INDEX = new RepresentationType( "index" ),
            NODE_INDEX_ROOT = new RepresentationType( "node-index" ),
            RELATIONSHIP_INDEX_ROOT = new RepresentationType( "relationship-index" ),
            INDEX_DEFINITION = new RepresentationType( "index-definition", "index-definitions", IndexDefinition.class ),
            CONSTRAINT_DEFINITION = new RepresentationType( "constraint-definition", "constraint-definitions", ConstraintDefinition.class ),
            PLUGINS = new RepresentationType( "plugins" ),
            PLUGIN = new RepresentationType( "plugin" ),
            PLUGIN_DESCRIPTION = new RepresentationType( "plugin-point" ),
            SERVER_PLUGIN_DESCRIPTION = new RepresentationType( "server-plugin", null ),
            PLUGIN_PARAMETER = new RepresentationType( "plugin-parameter", "plugin-parameter-list" ),
            // Value types
            URI = new RepresentationType( "uri", null ),
            TEMPLATE = new RepresentationType( "uri-template" ),
            STRING = new RepresentationType( "string", "strings", String.class ),
            // primitives
            BYTE = new RepresentationType( "byte", "bytes", byte.class ),
            CHAR = new RepresentationType( "character", "characters", char.class ),
            SHORT = new RepresentationType( "short", "shorts", short.class ),
            INTEGER = new RepresentationType( "integer", "integers", int.class ),
            LONG = new RepresentationType( "long", "longs", long.class ),
            FLOAT = new RepresentationType( "float", "floats", float.class ),
            DOUBLE = new RepresentationType( "double", "doubles", double.class ),
            BOOLEAN = new RepresentationType( "boolean", "booleans", boolean.class ),
            NOTHING = new RepresentationType( "void", null ),
            // System
            EXCEPTION = new RepresentationType( "exception" ),
            AUTHORIZATION = new RepresentationType( "authorization" ),
            MAP = new RepresentationType( "map", "maps", Map.class ),
            NULL = new RepresentationType( "null", "nulls", Object.class );


    final String valueName;
    final String listName;
    final Class<?> extend;

    private RepresentationType( String valueName, String listName )
    {
        this( valueName, listName, null );
    }

    private RepresentationType( String valueName, String listName, Class<?> extend )
    {
        this.valueName = valueName;
        this.listName = listName;
        this.extend = extend;
        if ( valueName != null )
        {
            types.put( valueName.replace( "-", "" ), this );
        }
        if ( extend != null )
        {
            extended.put( extend, this );
            if ( extend.isPrimitive() )
            {
                extended.put( boxed.get( extend ), this );
            }
        }
    }

    RepresentationType( String type )
    {
        if ( type == null )
        {
            throw new IllegalArgumentException( "type may not be null" );
        }
        this.valueName = type;
        this.listName = type + "s";
        this.extend = null;
    }

    @Override
    public String toString()
    {
        return valueName;
    }

    static RepresentationType valueOf( Class<? extends Number> type )
    {
        return types.get( type.getSimpleName().toLowerCase() );
    }

    @Override
    public int hashCode()
    {
        if ( valueName == null )
        {
            return listName.hashCode();
        }
        return valueName.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof RepresentationType )
        {
            RepresentationType that = (RepresentationType) obj;
            if ( this.valueName != null )
            {
                if ( valueName.equals( that.valueName ) )
                {
                    if ( this.listName != null )
                    {
                        return listName.equals( that.listName );
                    }
                    else
                    {
                        return that.listName == null;
                    }
                }
            }
            else if ( this.listName != null )
            {
                return that.valueName == null && listName.equals( that.listName );
            }
        }
        return false;
    }

    static RepresentationType extended( Class<?> extend )
    {
        return extended.get( extend );
    }
}
