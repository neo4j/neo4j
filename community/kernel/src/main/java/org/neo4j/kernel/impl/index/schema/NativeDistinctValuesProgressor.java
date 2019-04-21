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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;

import org.neo4j.index.internal.gbptree.Seeker;

public class NativeDistinctValuesProgressor<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexProgressor<KEY,VALUE>
{
    private final IndexLayout<KEY,VALUE> layout;
    private final KEY prev;
    private final Comparator<KEY> comparator;
    private boolean first = true;
    private long countForCurrentValue;
    private boolean last;

    NativeDistinctValuesProgressor( Seeker<KEY,VALUE> seeker, EntityValueClient client, IndexLayout<KEY,VALUE> layout,
            Comparator<KEY> comparator )
    {
        super( seeker, client );
        this.layout = layout;
        prev = layout.newKey();
        this.comparator = comparator;
    }

    @Override
    public boolean next()
    {
        try
        {
            while ( seeker.next() )
            {
                KEY key = seeker.key();
                if ( first )
                {
                    first = false;
                    countForCurrentValue = 1;
                    layout.copyKey( key, prev );
                }
                else if ( comparator.compare( prev, key ) == 0 )
                {
                    // same as previous
                    countForCurrentValue++;
                }
                else
                {
                    // different from previous
                    boolean accepted = client.acceptEntity( countForCurrentValue, Float.NaN, extractValues( prev ) );
                    countForCurrentValue = 1;
                    layout.copyKey( key, prev );
                    if ( accepted )
                    {
                        return true;
                    }
                }
            }
            boolean finalResult = !first && !last && client.acceptEntity( countForCurrentValue, Float.NaN, extractValues( prev ) );
            last = true;
            return finalResult;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
