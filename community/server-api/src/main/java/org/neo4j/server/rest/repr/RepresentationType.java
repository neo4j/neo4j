/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.values.storable.DateTimeValue;

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
    public static final RepresentationType GRAPHDB =
            new RepresentationType( "graphdb", null, GraphDatabaseService.class );
    public static final RepresentationType NODE = new RepresentationType( "node", "nodes", Node.class );
    public static final RepresentationType RELATIONSHIP =
            new RepresentationType( "relationship", "relationships", Relationship.class );
    public static final RepresentationType PATH = new RepresentationType( "path", "paths", Path.class );
    public static final RepresentationType FULL_PATH =
            new RepresentationType( "full-path", "full-paths", FullPath.class );
    public static final RepresentationType RELATIONSHIP_TYPE =
            new RepresentationType( "relationship-type", "relationship-types", RelationshipType.class );
    public static final RepresentationType PROPERTIES = new RepresentationType( "properties" );
    public static final RepresentationType INDEX = new RepresentationType( "index" );
    public static final RepresentationType NODE_INDEX_ROOT = new RepresentationType( "node-index" );
    public static final RepresentationType RELATIONSHIP_INDEX_ROOT = new RepresentationType( "relationship-index" );
    public static final RepresentationType INDEX_DEFINITION =
            new RepresentationType( "index-definition", "index-definitions", IndexDefinition.class );
    public static final RepresentationType CONSTRAINT_DEFINITION =
            new RepresentationType( "constraint-definition", "constraint-definitions", ConstraintDefinition.class );
    public static final RepresentationType PLUGINS = new RepresentationType( "plugins" );
    public static final RepresentationType PLUGIN = new RepresentationType( "plugin" );
    public static final RepresentationType PLUGIN_DESCRIPTION = new RepresentationType( "plugin-point" );
    public static final RepresentationType SERVER_PLUGIN_DESCRIPTION = new RepresentationType( "server-plugin", null );
    public static final RepresentationType PLUGIN_PARAMETER =
            new RepresentationType( "plugin-parameter", "plugin-parameter-list" );
    public static final RepresentationType URI = new RepresentationType( "uri", null );
    public static final RepresentationType TEMPLATE = new RepresentationType( "uri-template" );
    public static final RepresentationType STRING = new RepresentationType( "string", "strings", String.class );
    public static final RepresentationType POINT = new RepresentationType( "point", "points", Point.class );
    public static final RepresentationType TEMPORAL = new RepresentationType( "temporal", "temporals", Temporal.class );
    public static final RepresentationType TEMPORAL_AMOUNT = new RepresentationType( "temporal-amount", "temporal-amounts", TemporalAmount.class );
    public static final RepresentationType BYTE = new RepresentationType( "byte", "bytes", byte.class );
    public static final RepresentationType CHAR = new RepresentationType( "character", "characters", char.class );
    public static final RepresentationType SHORT = new RepresentationType( "short", "shorts", short.class );
    public static final RepresentationType INTEGER = new RepresentationType( "integer", "integers", int.class );
    public static final RepresentationType LONG = new RepresentationType( "long", "longs", long.class );
    public static final RepresentationType FLOAT = new RepresentationType( "float", "floats", float.class );
    public static final RepresentationType DOUBLE = new RepresentationType( "double", "doubles", double.class );
    public static final RepresentationType BOOLEAN = new RepresentationType( "boolean", "booleans", boolean.class );
    public static final RepresentationType NOTHING = new RepresentationType( "void", null );
    public static final RepresentationType EXCEPTION = new RepresentationType( "exception" );
    public static final RepresentationType AUTHORIZATION = new RepresentationType( "authorization" );
    public static final RepresentationType MAP = new RepresentationType( "map", "maps", Map.class );
    public static final RepresentationType NULL = new RepresentationType( "null", "nulls", Object.class );

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
