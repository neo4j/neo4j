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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;

public class PropertyNameUtils
{
    private PropertyNameUtils()
    {
    }

    public static String[] getPropertyKeys( TokenRead tokenRead, int...properties )
            throws PropertyKeyIdNotFoundKernelException
    {
        String[] propertyKeys = new String[properties.length];
        for ( int i = 0; i < properties.length; i++ )
        {
            propertyKeys[i] = tokenRead.propertyKeyName( properties[i] );
        }
        return propertyKeys;
    }

    public static String[] getPropertyKeys( TokenNameLookup tokenNameLookup, LabelSchemaDescriptor descriptor )
    {
        int[] propertyKeyIds = descriptor.getPropertyIds();
        String[] propertyKeys = new String[propertyKeyIds.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            propertyKeys[i] = tokenNameLookup.propertyKeyGetName( propertyKeyIds[i] );
        }
        return propertyKeys;
    }

    public static String[] getPropertyKeys( TokenNameLookup tokenNameLookup, int[] propertyIds )
    {
        String[] propertyKeys = new String[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            propertyKeys[i] = tokenNameLookup.propertyKeyGetName( propertyIds[i] );
        }
        return propertyKeys;
    }

    public static int[] getPropertyIds( TokenRead tokenRead, String[] propertyKeys )
    {
        int[] propertyKeyIds = new int[propertyKeys.length];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeyIds[i] = tokenRead.propertyKey( propertyKeys[i] );
        }
        return propertyKeyIds;
    }

    public static int[] getPropertyIds( TokenRead tokenRead, Iterable<String> propertyKeys )
    {
        return Iterables.stream( propertyKeys ).mapToInt( tokenRead::propertyKey ).toArray();
    }

    public static int[] getOrCreatePropertyKeyIds( TokenWrite tokenWrite, String... propertyKeys )
            throws IllegalTokenNameException
    {
        int[] propertyKeyIds = new int[propertyKeys.length];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeyIds[i] = tokenWrite.propertyKeyGetOrCreateForName( propertyKeys[i] );
        }
        return propertyKeyIds;
    }

    public static int[] getOrCreatePropertyKeyIds( TokenWrite tokenWrite, IndexDefinition indexDefinition )
            throws IllegalTokenNameException
    {
        ArrayList<Integer> propertyKeyIds = new ArrayList<>();
        for ( String s : indexDefinition.getPropertyKeys() )
        {
            propertyKeyIds.add( tokenWrite.propertyKeyGetOrCreateForName( s ) );
        }
        return propertyKeyIds.stream().mapToInt( i -> i ).toArray();
    }
}
