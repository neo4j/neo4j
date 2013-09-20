/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

/**
 * Validates data integrity during the prepare phase of {@link WriteTransaction}.
 */
public class IntegrityValidator
{
    private final NeoStore neoStore;

    public IntegrityValidator(NeoStore neoStore)
    {
        this.neoStore = neoStore;
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
        if( lastCommittedTxWhenTransactionStarted < neoStore.getLatestConstraintIntroducingTx() )
        {
            // Constraints have changed since the transaction begun

            // This should be a relatively uncommon case, window for this happening is a few milliseconds when an admin
            // explicitly creates a constraint, after the index has been populated. We can improve this later on by
            // replicating the constraint validation logic down here, or rethinking where we validate constraints.
            // For now, we just kill these transactions.
            throw Exceptions.withCause( new XAException( XAException.XA_RBINTEGRITY ),
                    new ConstraintViolationException(
                            "Database constraints have changed after this transaction started, which is not yet " +
                            "supported. Please retry your transaction to ensure all constraints are executed." ) );
        }
    }
}
