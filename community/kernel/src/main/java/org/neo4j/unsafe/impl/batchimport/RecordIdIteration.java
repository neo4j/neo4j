package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class RecordIdIteration
{
    public static final PrimitiveLongIterator backwards( long highExcluded, long lowIncluded )
    {
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private long next = highExcluded - 1;

            @Override
            protected boolean fetchNext()
            {
                return next >= lowIncluded ? next( next-- ) : false;
            }
        };
    }

    public static final PrimitiveLongIterator forwards( long lowIncluded, long highExcluded )
    {
        return new PrimitiveLongCollections.PrimitiveLongBaseIterator()
        {
            private long nextId = lowIncluded;

            @Override
            protected boolean fetchNext()
            {
                return nextId < highExcluded ? next( nextId++ ) : false;
            }
        };
    }

    public static PrimitiveLongIterator allIn( RecordStore<? extends AbstractBaseRecord> store )
    {
        return forwards( store.getNumberOfReservedLowIds(), store.getHighId() );
    }
}
