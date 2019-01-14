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
package org.neo4j.kernel.impl.api.index.sampling;

import org.neo4j.storageengine.api.schema.IndexSample;

/**
 * Builds index sample.
 * It's implementation specific how sample will be build: using index directly or based on samples
 * provided through various include/exclude calls
 * @see DefaultNonUniqueIndexSampler
 */
public interface NonUniqueIndexSampler
{
    void include( String value );

    void include( String value, long increment );

    void exclude( String value );

    void exclude( String value, long decrement );

    IndexSample result();

    IndexSample result( int numDocs );

    abstract class Adapter implements NonUniqueIndexSampler
    {
        @Override
        public void include( String value )
        {   // no-op
        }

        @Override
        public void include( String value, long increment )
        {   // no-op
        }

        @Override
        public void exclude( String value )
        {   // no-op
        }

        @Override
        public void exclude( String value, long decrement )
        {   // no-op
        }
    }
}
