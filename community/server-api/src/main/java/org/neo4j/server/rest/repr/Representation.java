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

import java.net.URI;

public abstract class Representation
{
    // non-inlineable constants
    public static final String GRAPHDB = RepresentationType.GRAPHDB.valueName;
    public static final String INDEX = RepresentationType.INDEX.valueName;
    public static final String NODE_INDEXES = RepresentationType.NODE_INDEX_ROOT.valueName;
    public static final String RELATIONSHIP_INDEXES = RepresentationType.RELATIONSHIP_INDEX_ROOT.valueName;
    public static final String NODE = RepresentationType.NODE.valueName;
    public static final String NODE_LIST = RepresentationType.NODE.listName;
    public static final String RELATIONSHIP = RepresentationType.RELATIONSHIP.valueName;
    public static final String RELATIONSHIP_LIST = RepresentationType.RELATIONSHIP.listName;
    public static final String PATH = RepresentationType.PATH.valueName;
    public static final String PATH_LIST = RepresentationType.PATH.listName;
    public static final String RELATIONSHIP_TYPE = RepresentationType.RELATIONSHIP_TYPE.valueName;
    public static final String PROPERTIES_MAP = RepresentationType.PROPERTIES.valueName;
    public static final String EXTENSIONS_MAP = RepresentationType.PLUGINS.valueName;
    public static final String EXTENSION = RepresentationType.PLUGIN.valueName;
    public static final String URI = RepresentationType.URI.valueName;
    public static final String URI_TEMPLATE = RepresentationType.TEMPLATE.valueName;
    public static final String STRING = RepresentationType.STRING.valueName;
    public static final String STRING_LIST = RepresentationType.STRING.listName;
    public static final String BYTE = RepresentationType.BYTE.valueName;
    public static final String BYTE_LIST = RepresentationType.BYTE.listName;
    public static final String CHARACTER = RepresentationType.CHAR.valueName;
    public static final String CHARACTER_LIST = RepresentationType.CHAR.listName;
    public static final String SHORT = RepresentationType.SHORT.valueName;
    public static final String SHORT_LIST = RepresentationType.SHORT.listName;
    public static final String INTEGER = RepresentationType.INTEGER.valueName;
    public static final String INTEGER_LIST = RepresentationType.INTEGER.listName;
    public static final String LONG = RepresentationType.LONG.valueName;
    public static final String LONG_LIST = RepresentationType.LONG.listName;
    public static final String FLOAT = RepresentationType.FLOAT.valueName;
    public static final String FLOAT_LIST = RepresentationType.FLOAT.listName;
    public static final String DOUBLE = RepresentationType.DOUBLE.valueName;
    public static final String DOUBLE_LIST = RepresentationType.DOUBLE.listName;
    public static final String BOOLEAN = RepresentationType.BOOLEAN.valueName;
    public static final String BOOLEAN_LIST = RepresentationType.BOOLEAN.listName;
    public static final String EXCEPTION = RepresentationType.EXCEPTION.valueName;
    public static final String MAP = RepresentationType.MAP.valueName;

    final RepresentationType type;

    Representation( RepresentationType type )
    {
        this.type = type;
    }

    Representation( String type )
    {
        this.type = new RepresentationType( type );
    }

	public RepresentationType getRepresentationType() 
	{
		return this.type;
	}
	
    abstract String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions );

    abstract void addTo( ListSerializer serializer );

    abstract void putTo( MappingSerializer serializer, String key );

    boolean isEmpty()
    {
        return false;
    }

    public static Representation emptyRepresentation()
    {
        return new Representation( (RepresentationType) null )
        {
            @Override
            boolean isEmpty()
            {
                return true;
            }

            @Override
            String serialize( RepresentationFormat format, URI baseUri, ExtensionInjector extensions )
            {
                return "";
            }

            @Override
            void putTo( MappingSerializer serializer, String key )
            {
            }

            @Override
            void addTo( ListSerializer serializer )
            {
            }
        };
    }
}
