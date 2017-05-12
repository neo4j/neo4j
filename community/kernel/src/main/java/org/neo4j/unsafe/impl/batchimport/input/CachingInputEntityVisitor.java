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
package org.neo4j.unsafe.impl.batchimport.input;

import java.util.ArrayList;
import java.util.List;

public class CachingInputEntityVisitor implements InputEntityVisitor
{
    private final InputEntityVisitor delegate;

    public CachingInputEntityVisitor( InputEntityVisitor delegate )
    {
        this.delegate = delegate;
    }

    public CachingInputEntityVisitor()
    {
        this( new InputEntityVisitor.Adapter() );
    }

    public boolean hasPropertyId;
    public long propertyId;
    public final List<Object> properties = new ArrayList<>();

    public boolean hasLongId;
    public long longId;
    public Object objectId;
    public Group idGroup;

    public final List<String> labels = new ArrayList<>();
    public boolean hasLabelField;
    public long labelField;

    public boolean hasLongStartId;
    public long longStartId;
    public Object objectStartId;
    public Group startIdGroup;

    public boolean hasLongEndId;
    public long longEndId;
    public Object objectEndId;
    public Group endIdGroup;

    public boolean hasIntType;
    public int intType;
    public String stringType;

    @Override
    public boolean propertyId( long nextProp )
    {
        hasPropertyId = true;
        propertyId = nextProp;
        return delegate.propertyId( nextProp );
    }

    @Override
    public boolean property( String key, Object value )
    {
        properties.add( key );
        properties.add( value );
        return delegate.property( key, value );
    }

    @Override
    public boolean id( long id )
    {
        hasLongId = true;
        longId = id;
        return delegate.id( id );
    }

    @Override
    public boolean id( Object id, Group group )
    {
        objectId = id;
        idGroup = group;
        return delegate.id( id, group );
    }

    @Override
    public boolean labels( String[] labels )
    {
        for ( String label : labels )
        {
            this.labels.add( label );
        }
        return delegate.labels( labels );
    }

    @Override
    public boolean labelField( long labelField )
    {
        hasLabelField = true;
        this.labelField = labelField;
        return delegate.labelField( labelField );
    }

    @Override
    public boolean startId( long id )
    {
        hasLongStartId = true;
        longStartId = id;
        return delegate.startId( id );
    }

    @Override
    public boolean startId( Object id, Group group )
    {
        objectStartId = id;
        startIdGroup = group;
        return delegate.startId( id, group );
    }

    @Override
    public boolean endId( long id )
    {
        hasLongEndId = true;
        longEndId = id;
        return delegate.endId( id );
    }

    @Override
    public boolean endId( Object id, Group group )
    {
        objectEndId = id;
        endIdGroup = group;
        return delegate.endId( id, group );
    }

    @Override
    public boolean type( int type )
    {
        hasIntType = true;
        intType = type;
        return delegate.type( type );
    }

    @Override
    public boolean type( String type )
    {
        stringType = type;
        return delegate.type( type );
    }

    @Override
    public void endOfEntity()
    {
        hasPropertyId = false;
        properties.clear();
        hasLongId = false;
        labels.clear();
        hasLabelField = false;
        hasLongStartId = false;
        hasLongEndId = false;
        hasIntType = false;
        delegate.endOfEntity();
    }

    public String[] labels()
    {
        return labels.toArray( new String[labels.size()] );
    }

    public Object[] properties()
    {
        return properties.toArray();
    }

    public Object id()
    {
        return hasLongId ? longId : objectId;
    }

    public Object endId()
    {
        return hasLongEndId ? longEndId : objectEndId;
    }

    public Object startId()
    {
        return hasLongStartId ? longStartId : objectStartId;
    }
}
