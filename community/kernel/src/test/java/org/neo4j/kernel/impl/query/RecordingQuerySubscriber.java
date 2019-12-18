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
package org.neo4j.kernel.impl.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.values.AnyValue;

public class RecordingQuerySubscriber implements QuerySubscriber
{
    private List<AnyValue[]> all = new ArrayList<>(  );
    private AnyValue[] current;
    private Throwable throwable;
    private QueryStatistics statistics;

    @Override
    public void onResult( int numberOfFields )
    {
        this.current = new AnyValue[numberOfFields];
    }

    @Override
    public void onRecord()
    {
    }

    @Override
    public void onField( int offset, AnyValue value )
    {
        current[offset] = value;
    }

    @Override
    public void onRecordCompleted()
    {
        all.add( Arrays.copyOf( current, current.length ) );
    }

    @Override
    public void onError( Throwable throwable )
    {
        if ( this.throwable == null )
        {
            this.throwable = throwable;
        }
    }

    @Override
    public void onResultCompleted( QueryStatistics statistics )
    {
        this.statistics = statistics;
    }

    public List<AnyValue[]> getOrThrow() throws Throwable
    {
        assertNoErrors();
        return all;
    }

    public void assertNoErrors() throws Throwable
    {
        if ( throwable != null )
        {
            throw throwable;
        }
    }

    public QueryStatistics queryStatistics()
    {
        return statistics;
    }
}
