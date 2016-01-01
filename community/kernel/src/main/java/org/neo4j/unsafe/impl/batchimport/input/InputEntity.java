/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.Pair;

import static java.lang.String.format;

/**
 * Represents an entity from an input source, for example a .csv file.
 */
public abstract class InputEntity
{
    private final Object[] properties;
    private final Long firstPropertyId;

    public InputEntity( Object[] properties, Long firstPropertyId )
    {
        this.properties = properties;
        this.firstPropertyId = firstPropertyId;
    }

    public Object[] properties()
    {
        return properties;
    }

    public boolean hasFirstPropertyId()
    {
        return firstPropertyId != null;
    }

    public long firstPropertyId()
    {
        return firstPropertyId;
    }

    @Override
    public String toString()
    {
        Collection<Pair<String,?>> fields = new ArrayList<>();
        toStringFields( fields );

        StringBuilder builder = new StringBuilder( "%s:" );
        Object[] arguments = new Object[fields.size()+1];
        int cursor = 0;
        arguments[cursor++] = getClass().getSimpleName();
        for ( Pair<String, ?> item : fields )
        {
            builder.append( "%n   %s" );
            arguments[cursor++] = item.first() + ": " + item.other();
        }

        return format( builder.toString(), arguments );
    }

    protected void toStringFields( Collection<Pair<String, ?>> fields )
    {
        if ( hasFirstPropertyId() )
        {
            fields.add( Pair.of( "nextProp", firstPropertyId ) );
        }
        else
        {
            fields.add( Pair.of( "properties", Arrays.toString( properties ) ) );
        }
    }
}
