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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.VersionedHashMap;

/**
 * Overlays transaction state on a {@link PropertyItem} cursors.
 */
public abstract class TxAbstractPropertyCursor implements Cursor<PropertyItem>, PropertyItem
{
    private final Consumer<TxAbstractPropertyCursor> instanceCache;

    protected Cursor<PropertyItem> cursor;
    protected VersionedHashMap<Integer, DefinedProperty> addedProperties;
    protected VersionedHashMap<Integer, DefinedProperty> changedProperties;
    protected VersionedHashMap<Integer, DefinedProperty> removedProperties;

    protected DefinedProperty property;

    public TxAbstractPropertyCursor( Consumer<TxAbstractPropertyCursor> instanceCache )
    {
        this.instanceCache = instanceCache;
    }

    public Cursor<PropertyItem> init( Cursor<PropertyItem> cursor,
            VersionedHashMap<Integer, DefinedProperty> addedProperties,
            VersionedHashMap<Integer, DefinedProperty> changedProperties,
            VersionedHashMap<Integer, DefinedProperty> removedProperties )
    {
        this.cursor = cursor;
        this.addedProperties = addedProperties;
        this.changedProperties = changedProperties;
        this.removedProperties = removedProperties;

        return this;
    }

    @Override
    public PropertyItem get()
    {
        if ( property == null )
        {
            throw new IllegalStateException();
        }

        return this;
    }

    @Override
    public void close()
    {
        cursor.close();
        cursor = null;
        property = null;
        instanceCache.accept( this );
    }

    @Override
    public int propertyKeyId()
    {
        return property.propertyKeyId();
    }

    @Override
    public Object value()
    {
        return property.value();
    }
}
