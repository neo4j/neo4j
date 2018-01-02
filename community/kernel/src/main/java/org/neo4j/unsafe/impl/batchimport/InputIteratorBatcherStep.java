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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.unsafe.impl.batchimport.staging.IteratorBatcherStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * {@link IteratorBatcherStep} that is tailored to the {@link BatchImporter} as it produces {@link Batch}
 * objects.
 */
public class InputIteratorBatcherStep<T> extends IteratorBatcherStep<T>
{
    public InputIteratorBatcherStep( StageControl control, Configuration config,
            InputIterator<T> data, Class<T> itemClass )
    {
        super( control, config, data, itemClass );
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    @Override
    protected Object nextBatchOrNull( long ticket, int batchSize )
    {
        Object batch = super.nextBatchOrNull( ticket, batchSize );
        return batch != null ? new Batch( (Object[]) batch ) : null;
    }
}
