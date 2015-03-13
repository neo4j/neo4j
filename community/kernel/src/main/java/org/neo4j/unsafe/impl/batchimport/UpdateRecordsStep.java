/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Updates a batch of records to a store. Can be given a {@link Predicate} that can choose to not
 * {@link Predicate#accept(Object) accept} a record, which will have that record be written as unused instead.
 */
public class UpdateRecordsStep<RECORD extends AbstractBaseRecord> extends ExecutorServiceStep<RECORD[]>
{
    private final RecordStore<RECORD> store;
    private final Predicate<RECORD> updatePredicate;

    public UpdateRecordsStep( StageControl control, int workAheadSize, int movingAverageSize,
            RecordStore<RECORD> store )
    {
        this( control, workAheadSize, movingAverageSize, store, Predicates.<RECORD>TRUE() );
    }

    public UpdateRecordsStep( StageControl control, int workAheadSize, int movingAverageSize,
            RecordStore<RECORD> store, Predicate<RECORD> updatePredicate )
    {
        super( control, "v", workAheadSize, movingAverageSize, 1 );
        this.store = store;
        this.updatePredicate = updatePredicate;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Object process( long ticket, RECORD[] batch )
    {
        for ( RECORD record : batch )
        {
            if ( record.inUse() && !updatePredicate.accept( record ) )
            {
                record = (RECORD) record.clone();
                record.setInUse( false );
            }
            store.updateRecord( record );
        }
        return null; // end of line
    }
}
