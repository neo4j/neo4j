/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.index;

import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.io.pagecache.context.CursorContext;

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

    IndexSample sample( CursorContext cursorContext, AtomicBoolean stopped );

    IndexSample sample( int numDocs, CursorContext cursorContext );

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
