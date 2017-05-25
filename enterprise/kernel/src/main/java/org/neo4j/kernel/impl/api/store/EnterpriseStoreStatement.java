package org.neo4j.kernel.impl.api.store;

import java.util.function.Supplier;

import org.neo4j.kernel.impl.api.IndexReaderFactory;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.storageengine.api.schema.LabelScanReader;

public class EnterpriseStoreStatement extends StoreStatement
{
    public EnterpriseStoreStatement( NeoStores neoStores, Supplier<IndexReaderFactory> indexReaderFactory,
            Supplier<LabelScanReader> labelScanReaderSupplier, LockService lockService )
    {
        super( neoStores, indexReaderFactory, labelScanReaderSupplier, lockService );
    }
}
