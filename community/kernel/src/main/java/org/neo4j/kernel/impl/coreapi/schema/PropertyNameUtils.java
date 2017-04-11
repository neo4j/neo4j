/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;

public class PropertyNameUtils
{
    private PropertyNameUtils()
    {
    }

    public static String[] getPropertyKeys( ReadOperations readOperations, LabelSchemaDescriptor descriptor )
            throws PropertyKeyIdNotFoundKernelException
    {
        int[] propertyKeyIds = descriptor.getPropertyIds();
        String[] propertyKeys = new String[propertyKeyIds.length];
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            propertyKeys[i] = readOperations.propertyKeyGetName( propertyKeyIds[i] );
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

    public static String[] getPropertyKeys( ReadOperations readOperations, int[] propertyIds )
            throws PropertyKeyIdNotFoundKernelException
    {
        String[] propertyKeys = new String[propertyIds.length];
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            propertyKeys[i] = readOperations.propertyKeyGetName( propertyIds[i] );
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

    public static int[] getPropertyIds( ReadOperations statement, String[] propertyKeys )
    {
        int[] propertyKeyIds = new int[propertyKeys.length];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeyIds[i] = statement.propertyKeyGetForName( propertyKeys[i] );
        }
        return propertyKeyIds;
    }

    public static int[] getPropertyIds( ReadOperations statement, Iterable<String> propertyKeys )
    {
        return Iterables.stream( propertyKeys ).mapToInt( statement::propertyKeyGetForName ).toArray();
    }

    public static int[] getOrCreatePropertyKeyIds( TokenWriteOperations statement, String... propertyKeys )
            throws IllegalTokenNameException
    {
        int[] propertyKeyIds = new int[propertyKeys.length];
        for ( int i = 0; i < propertyKeys.length; i++ )
        {
            propertyKeyIds[i] = statement.propertyKeyGetOrCreateForName( propertyKeys[i] );
        }
        return propertyKeyIds;
    }

    public static int[] getOrCreatePropertyKeyIds( TokenWriteOperations statement, IndexDefinition indexDefinition )
            throws IllegalTokenNameException
    {
        ArrayList<Integer> propertyKeyIds = new ArrayList<>();
        for ( String s : indexDefinition.getPropertyKeys() )
        {
            propertyKeyIds.add( statement.propertyKeyGetOrCreateForName( s ) );
        }
        return propertyKeyIds.stream().mapToInt( i -> i ).toArray();
    }
}
