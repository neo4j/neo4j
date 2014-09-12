/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.CombiningIterator;
import org.neo4j.helpers.collection.FilteringIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.VersionedHashMap;

import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

public class PropertyContainerState extends EntityState
{
    private static final ResourceIterator<DefinedProperty> NO_PROPERTIES = emptyIterator();

    private VersionedHashMap<Integer, DefinedProperty> addedProperties;
    private VersionedHashMap<Integer, DefinedProperty> changedProperties;
    private VersionedHashMap<Integer, DefinedProperty> removedProperties;

    private final Predicate<DefinedProperty> excludePropertiesWeKnowAbout = new Predicate<DefinedProperty>()
    {
        @Override
        public boolean accept( DefinedProperty item )
        {
            return (removedProperties == null || !removedProperties.containsKey( item.propertyKeyId() ))
                && (addedProperties == null || !addedProperties.containsKey( item.propertyKeyId() ))
                && (changedProperties == null || !changedProperties.containsKey( item.propertyKeyId() ));
        }
    };

    public interface Visitor
    {
        void visitPropertyChanges( long entityId, Iterator<DefinedProperty> added, Iterator<DefinedProperty> changed,
                                                  Iterator<Integer> removed );
    }

    public PropertyContainerState( long id )
    {
        super( id );
    }

    public void clear()
    {
        if(changedProperties != null) changedProperties.clear();
        if(addedProperties != null) addedProperties.clear();
        if(removedProperties != null) removedProperties.clear();
    }

    public void changeProperty( DefinedProperty property )
    {
        if(addedProperties != null)
        {
            if(addedProperties.containsKey( property.propertyKeyId() ))
            {
                addedProperties.put( property.propertyKeyId(), property );
                return;
            }
        }

        if(changedProperties == null)
        {
            changedProperties = new VersionedHashMap<>();
        }
        changedProperties.put( property.propertyKeyId(), property );
        if(removedProperties != null)
        {
            removedProperties.remove( property.propertyKeyId() );
        }
    }

    public void addProperty( DefinedProperty property )
    {
        if(removedProperties != null)
        {
            DefinedProperty removed = removedProperties.remove( property.propertyKeyId() );
            if(removed != null)
            {
                // This indicates the user did remove+add as two discrete steps, which should be translated to
                // a single change operation.
                changeProperty( property );
                return;
            }
        }
        if(addedProperties == null)
        {
            addedProperties = new VersionedHashMap<>();
        }
        addedProperties.put( property.propertyKeyId(), property );

    }

    public void removeProperty( DefinedProperty property )
    {
        if(addedProperties != null)
        {
            if(addedProperties.remove( property.propertyKeyId() ) != null)
            {
                return;
            }
        }
        if(removedProperties == null)
        {
            removedProperties = new VersionedHashMap<>();
        }
        removedProperties.put( property.propertyKeyId(), property );
        if(changedProperties != null)
        {
            changedProperties.remove( property.propertyKeyId() );
        }
    }

    public Iterator<DefinedProperty> addedProperties()
    {
        return addedProperties != null ? addedProperties.values().iterator() : NO_PROPERTIES;
    }

    public Iterator<DefinedProperty> changedProperties()
    {
        return changedProperties != null ? changedProperties.values().iterator() : NO_PROPERTIES;
    }

    public Iterator<Integer> removedProperties()
    {
        return removedProperties != null ? removedProperties.keySet().iterator() : IteratorUtil.<Integer>emptyIterator();
    }

    public Iterator<DefinedProperty> addedAndChangedProperties()
    {
        Iterator<DefinedProperty> out = null;
        if(addedProperties != null)
        {
            out = addedProperties.values().iterator();
        }
        if(changedProperties != null)
        {
            if(out != null)
            {
                out = new CombiningIterator<>( IteratorUtil.iterator(out, changedProperties.values().iterator()) );
            }
            else
            {
                out = changedProperties.values().iterator();
            }
        }
        return out != null ? out : NO_PROPERTIES;
    }


    public Iterator<DefinedProperty> augmentProperties( Iterator<DefinedProperty> iterator )
    {
        if(removedProperties != null || addedProperties != null || changedProperties != null)
        {
            iterator = new FilteringIterator<>( iterator, excludePropertiesWeKnowAbout );

            if(addedProperties != null && addedProperties.size() > 0 )
            {
                iterator = new CombiningIterator<>( IteratorUtil.iterator( iterator, addedProperties.values().iterator()));
            }
            if(changedProperties != null && changedProperties.size() > 0 )
            {
                iterator = new CombiningIterator<>( IteratorUtil.iterator( iterator, changedProperties.values().iterator()));
            }
        }

        return iterator;
    }
    public void accept( Visitor visitor )
    {
        if(addedProperties != null || removedProperties != null || changedProperties != null)
        {
            visitor.visitPropertyChanges( getId(), addedProperties(), changedProperties(), removedProperties());
        }
    }
}
