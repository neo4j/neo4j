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
package org.neo4j.kernel.impl.api;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDatabaseKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.impl.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction;

public class KernelStatement implements TxState.Holder, Statement
{
    private final KernelTransactionImplementation transaction;
    protected final LockHolder lockHolder;
    protected final TxState.Holder txStateHolder;
    protected final IndexReaderFactory indexReaderFactory;
    protected final LabelScanStore labelScanStore;
    protected NeoStoreTransaction neoStoreTransaction;
    
    private LabelScanReader labelScanReader;
    private int referenceCount;
    private final OperationsFacade facade;
    private boolean closed;

    public KernelStatement( KernelTransactionImplementation transaction, IndexReaderFactory indexReaderFactory,
                            LabelScanStore labelScanStore,
                            TxState.Holder txStateHolder, LockHolder lockHolder, LegacyKernelOperations
                            legacyKernelOperations, StatementOperationParts operations,
                            NeoStoreTransaction neoStoreTransaction )
    {
        this.transaction = transaction;
        this.lockHolder = lockHolder;
        this.indexReaderFactory = indexReaderFactory;
        this.txStateHolder = txStateHolder;
        this.labelScanStore = labelScanStore;
        this.neoStoreTransaction = neoStoreTransaction;
        this.facade = new OperationsFacade( this, legacyKernelOperations, operations );
    }

    @Override
    public ReadOperations readOperations()
    {
        return facade;
    }

    @Override
    public TokenWriteOperations tokenWriteOperations() throws ReadOnlyDatabaseKernelException
    {
        transaction.assertTokenWriteAllowed();
        return facade;
    }

    @Override
    public DataWriteOperations dataWriteOperations()
            throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException
    {
        transaction.upgradeToDataTransaction();
        return facade;
    }

    @Override
    public SchemaWriteOperations schemaWriteOperations()
        throws InvalidTransactionTypeKernelException, ReadOnlyDatabaseKernelException
    {
        transaction.upgradeToSchemaTransaction();
        return facade;
    }

    @Override
    public TxState txState()
    {
        return txStateHolder.txState();
    }

    @Override
    public boolean hasTxState()
    {
        return txStateHolder.hasTxState();
    }

    @Override
    public boolean hasTxStateWithChanges()
    {
        return txStateHolder.hasTxStateWithChanges();
    }

    @Override
    public void close()
    {
        if ( !closed && release() )
        {
            closed = true;
            indexReaderFactory.close();
            if ( null != labelScanReader )
            {
                labelScanReader.close();
            }
            transaction.releaseStatement( this );
        }
    }

    void assertOpen()
    {
        if ( closed )
        {
            throw new NotInTransactionException( "The statement has been closed." );
        }
    }

    public LockHolder locks()
    {
        return lockHolder;
    }

    public IndexReader getIndexReader( long indexId ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory.newReader( indexId );
    }

    public IndexReader getFreshIndexReader( long indexId ) throws IndexNotFoundKernelException
    {
        return indexReaderFactory.newUnCachedReader( indexId );
    }

    public LabelScanReader getLabelScanReader()
    {
        if ( labelScanReader == null )
        {
            labelScanReader = labelScanStore.newReader();
        }
        return labelScanReader;
    }

    final void acquire()
    {
        referenceCount++;
    }

    private boolean release()
    {
        return --referenceCount == 0;
    }

    final void forceClose()
    {
        referenceCount = 0;
        close();
    }
}
