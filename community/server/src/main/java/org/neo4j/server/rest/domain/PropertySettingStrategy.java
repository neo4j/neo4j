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
package org.neo4j.server.rest.domain;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.rest.web.PropertyValueException;

/**
 * Responsible for setting properties on primitive types.
 */
public class PropertySettingStrategy
{
    private final GraphDatabaseAPI db;

    public PropertySettingStrategy( GraphDatabaseAPI db )
    {
        this.db = db;
    }

    /**
     * Set all properties on an entity, deleting any properties that existed on the entity but not in the
     * provided map.
     *
     * @param entity
     * @param properties
     */
    public void setAllProperties( PropertyContainer entity, Map<String, Object> properties ) throws PropertyValueException

    {
        Map<String, Object> propsToSet = properties == null ?
                new HashMap<String, Object>() :
                properties;

        try ( Transaction tx = db.beginTx() )
        {
            setProperties( entity, properties );
            ensureHasOnlyTheseProperties( entity, propsToSet.keySet() );

            tx.success();
        }
    }

    private void ensureHasOnlyTheseProperties( PropertyContainer entity, Set<String> propertiesThatShouldExist )
    {
        for ( String entityPropertyKey : entity.getPropertyKeys() )
        {
            if( ! propertiesThatShouldExist.contains( entityPropertyKey ))
            {
                entity.removeProperty( entityPropertyKey );
            }
        }
    }

    public void setProperties( PropertyContainer entity, Map<String, Object> properties ) throws PropertyValueException
    {
        if ( properties != null )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( Map.Entry<String, Object> property : properties.entrySet() )
                {
                    setProperty( entity, property.getKey(), property.getValue() );
                }
                tx.success();
            }
        }
    }

    public void setProperty(PropertyContainer entity, String key, Object value) throws PropertyValueException
    {
        if ( value instanceof Collection )
        {
            if ( ((Collection<?>) value).size() == 0 )
            {
                // Special case: Trying to set an empty array property. We cannot determine the type
                // of the collection now, so we fall back to checking if there already is a collection
                // on the entity, and either leave it intact if it is empty, or set it to an empty collection
                // of the same type as the original
                Object currentValue = entity.getProperty( key, null );
                if(currentValue != null &&
                   currentValue.getClass().isArray())
                {
                    if ( Array.getLength( currentValue ) == 0 )
                    {
                        // Ok, leave it this way
                        return;
                    }
                    
                    value = emptyArrayOfType(currentValue.getClass().getComponentType());

                }
                else
                {
                    throw new PropertyValueException(
                            "Unable to set property '" + key + "' to an empty array, " +
                            "because, since there are no values of any type in it, " +
                            "and no pre-existing collection to infer type from, it is not possible " +
                            "to determine what type of array to store." );
                }
            }
            else
            {
                // Non-empty collection
                value = convertToNativeArray( (Collection<?>) value );
            }
        }

        try ( Transaction tx = db.beginTx() )
        {
            entity.setProperty( key, value );
            tx.success();
        }
        catch ( IllegalArgumentException e )
        {
            throw new PropertyValueException( "Could not set property \"" + key + "\", unsupported type: " + value );
        }
    }

    public Object convert( Object value ) throws PropertyValueException
    {
        if ( !(value instanceof Collection) )
        {
            return value;
        }

        if ( ((Collection<?>) value).size() == 0 )
        {
            throw new PropertyValueException(
                    "Unable to convert '" + value + "' to an empty array, " +
                            "because, since there are no values of any type in it, " +
                            "and no pre-existing collection to infer type from, it is not possible " +
                            "to determine what type of array to store." );
        }

        return convertToNativeArray( (Collection<?>) value );
    }

    private Object emptyArrayOfType( Class<?> cls ) throws PropertyValueException
    {
       return Array.newInstance( cls, 0);
    }

    public static Object convertToNativeArray( Collection<?> collection )
    {
        Object[] array = null;
        Iterator<?> objects = collection.iterator();
        for ( int i = 0; objects.hasNext(); i++ )
        {
            Object object = objects.next();
            if ( array == null )
            {
                array = (Object[]) Array.newInstance( object.getClass(),
                        collection.size() );
            }
            array[i] = object;
        }
        return array;
    }
}
