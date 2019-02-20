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
package org.neo4j.cypher.result;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.values.AnyValue;

public abstract class NaiveRuntimeResult implements RuntimeResult
{
    private int requestedRows;
    private int servedRows;
    private List<AnyValue[]> materializedResult;
    private final QuerySubscriber subscriber;

    protected NaiveRuntimeResult( QuerySubscriber subscriber )
    {
        this.subscriber = subscriber;
    }

    @Override
    public void request( long numberOfRows )
    {
        requestedRows = StrictMath.addExact( requestedRows, (int) numberOfRows );
    }

    @Override
    public void cancel()
    {
        //do nothing
    }

    @Override
    public boolean await() throws Exception
    {
        if ( materializedResult == null )
        {
            materializedResult = new ArrayList<>();
            accept( row -> {
                materializedResult.add( row.fields().clone() );
                row.release();
                return true;
            } );
        }
        for ( ; servedRows < requestedRows && servedRows < materializedResult.size(); servedRows++ )
        {
            subscriber.newRecord();
            AnyValue[] current = materializedResult.get( servedRows );
            for ( int offset = 0; offset < current.length; offset++ )
            {
                subscriber.onValue( offset, current[offset] );
            }
            subscriber.closeRecord();
        }

        subscriber.onCompleted( queryStatistics() );
        return servedRows < materializedResult.size();
    }
}
