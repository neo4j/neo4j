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
package org.neo4j.kernel.impl.nioneo.xa;

import javax.transaction.xa.XAException;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;

import static java.lang.String.format;

/**
 * Validates data integrity during the prepare phase of {@link NeoStoreTransaction}.
 */
public class IntegrityValidator
{
    private final NeoStore neoStore;
    private final IndexingService indexes;

    public IntegrityValidator( NeoStore neoStore, IndexingService indexes )
    {
        this.neoStore = neoStore;
        this.indexes = indexes;
    }

    public void validateNodeRecord( NodeRecord record ) throws XAException
    {
        if ( !record.inUse() && record.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            throw Exceptions.withCause( new XAException( XAException.XA_RBINTEGRITY ),
                    new ConstraintViolationException(
                            "Node record " + record + " still has relationships" ) );
        }
    }

    public void validateTransactionStartKnowledge( long lastCommittedTxWhenTransactionStarted )
            throws XAException
    {
        long latestConstraintIntroducingTx = neoStore.getLatestConstraintIntroducingTx();
        if( lastCommittedTxWhenTransactionStarted < latestConstraintIntroducingTx )
        {
            // Constraints have changed since the transaction begun

            // This should be a relatively uncommon case, window for this happening is a few milliseconds when an admin
            // explicitly creates a constraint, after the index has been populated. We can improve this later on by
            // replicating the constraint validation logic down here, or rethinking where we validate constraints.
            // For now, we just kill these transactions.
            throw Exceptions.withCause( new XAException( XAException.XA_RBINTEGRITY ),
                    new ConstraintViolationException( format(
                            "Database constraints have changed (txId=%d) after this transaction (txId%d) started, " +
                            "which is not yet supported. Please retry your transaction to ensure all " +
                            "constraints are executed.", latestConstraintIntroducingTx,
                            lastCommittedTxWhenTransactionStarted ) ) );
        }
    }

    public void validateSchemaRule( SchemaRule schemaRule ) throws XAException
    {
        if(schemaRule instanceof UniquenessConstraintRule )
        {
            try
            {
                indexes.validateIndex( ((UniquenessConstraintRule)schemaRule).getOwnedIndex() );
            }
            catch ( ConstraintVerificationFailedKernelException e )
            {
                throw Exceptions.withCause( new XAException( XAException.XA_RBINTEGRITY ), e);
            }
            catch ( IndexNotFoundKernelException | IndexPopulationFailedKernelException e )
            {
                // We don't expect this to occur, and if they do, it is because we are in a very bad state - out of
                // disk or index corruption, or similar. This will kill the database such that it can be shut down
                // and have recovery performed. It's the safest bet to avoid loosing data.
                throw Exceptions.withCause( new XAException( XAException.XAER_RMERR ), e);
            }
        }
    }
}
