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
package org.neo4j.kernel.impl.index.schema;

import java.util.Iterator;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

import static org.neo4j.values.storable.Values.stringValue;

class ResultCursor implements Seeker<GenericKey,NativeIndexValue>
{
    private static final IndexSpecificSpaceFillingCurveSettings specificSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
    private final Iterator<String> iterator;
    private int pos = -1;
    private GenericKey key;

    ResultCursor( Iterator<String> keys )
    {
        iterator = keys;
    }

    @Override
    public boolean next()
    {
        if ( iterator.hasNext() )
        {
            String current = iterator.next();
            pos++;
            key = new GenericKey( specificSettings );
            key.initialize( pos );
            key.initFromValue( 0, stringValue( current ), NativeIndexKey.Inclusion.NEUTRAL );
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public GenericKey key()
    {
        return key;
    }

    @Override
    public NativeIndexValue value()
    {
        return NativeIndexValue.INSTANCE;
    }
}
