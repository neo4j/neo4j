/*
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
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.locking.Locks;

public class KernelStatement implements TxStateHolder, Statement
{
    protected final Locks.Client locks;
    protected final TxStateHolder txStateHolder;
    protected final IndexReaderFactory indexReaderFactory;
    protected final LabelScanStore labelScanStore;
    private final KernelTransactionImplementation transaction;
    private final OperationsFacade facade;
    private LabelScanReader labelScanReader;
    private int referenceCount;
    private boolean closed;

    public KernelStatement( KernelTransactionImplementation transaction, IndexReaderFactory indexReaderFactory,
                            LabelScanStore labelScanStore, TxStateHolder txStateHolder, Locks.Client locks,
                            StatementOperationParts operations )
    {
        this.transaction = transaction;
        this.locks = locks;
        this.indexReaderFactory = indexReaderFactory;
        this.txStateHolder = txStateHolder;
        this.labelScanStore = labelScanStore;
        this.facade = new OperationsFacade( this, operations );
    }

    @Override
    public ReadOperations readOperations()
    {
        return facade;
    }

    @Override
    public TokenWriteOperations tokenWriteOperations()
    {
        return facade;
    }

    @Override
    public DataWriteOperations dataWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        transaction.upgradeToDataTransaction();
        return facade;
    }

    @Override
    public SchemaWriteOperations schemaWriteOperations()
            throws InvalidTransactionTypeKernelException
    {
        transaction.upgradeToSchemaTransaction();
        return facade;
    }

    @Override
    public TransactionState txState()
    {
        return txStateHolder.txState();
    }

    @Override
    public LegacyIndexTransactionState legacyIndexTxState()
    {
        return txStateHolder.legacyIndexTxState();
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
            cleanupResources();
        }
    }

    void assertOpen()
    {
        if ( closed )
        {
            throw new NotInTransactionException( "The statement has been closed." );
        }
        if ( transaction.shouldBeTerminated() )
        {
            throw new TransactionTerminatedException();
        }
    }

    public Locks.Client locks()
    {
        return locks;
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
        referenceCount--;

        return (referenceCount == 0);
    }

    final void forceClose()
    {
        if ( !closed )
        {
            closed = true;
            referenceCount = 0;

            cleanupResources();
        }
    }

    private void cleanupResources()
    {
        indexReaderFactory.close();

        if ( null != labelScanReader )
        {
            labelScanReader.close();
        }

        transaction.releaseStatement( this );
    }
}
