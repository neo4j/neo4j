/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.ReadableLogChannel;

public abstract class TxExtractor
{
    public abstract void extract( LogBuffer buffer );

    public abstract ReadableLogChannel extract();

    public static TxExtractor create( final ReadableLogChannel data )
    {
        return new TxExtractor()
        {
            @Override
            public ReadableLogChannel extract()
            {
                return data;
            }

            @Override
            public void extract( LogBuffer buffer )
            {
                try
                {
                    while ( data.hasMoreData() )
                    {
                        buffer.put( data.get() );
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
}
