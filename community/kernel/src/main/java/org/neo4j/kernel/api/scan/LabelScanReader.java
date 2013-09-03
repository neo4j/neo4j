package org.neo4j.kernel.api.scan;

import org.neo4j.kernel.impl.api.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.emptyPrimitiveLongIterator;

public interface LabelScanReader
{
    PrimitiveLongIterator nodesWithLabel( long labelId );

    void close();

    LabelScanReader EMPTY = new LabelScanReader()
    {
        @Override
        public PrimitiveLongIterator nodesWithLabel( long labelId )
        {
            return emptyPrimitiveLongIterator();
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    };
}
