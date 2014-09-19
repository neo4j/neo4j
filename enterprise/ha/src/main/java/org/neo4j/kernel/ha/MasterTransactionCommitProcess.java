/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.transaction.TransactionPropagator;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.NeoStoreInjectedTransactionValidator;

/**
 * Commit process on the master side in HA, where transactions either comes in from slaves committing,
 * or gets created and committed directly on the master.
 */
public class MasterTransactionCommitProcess implements TransactionCommitProcess
{
    private final TransactionPropagator pusher;
    private final NeoStoreInjectedTransactionValidator validator;
    private final TransactionRepresentationCommitProcess inner;

    public MasterTransactionCommitProcess( TransactionRepresentationCommitProcess commitProcess,
                                           TransactionPropagator pusher,
                                           NeoStoreInjectedTransactionValidator validator)
    {
        this.inner = commitProcess;
        this.pusher = pusher;
        this.validator = validator;
    }

    @Override
    public long commit( TransactionRepresentation representation, LockGroup locks ) throws TransactionFailureException
    {
        validator.assertInjectionAllowed( representation.getLatestCommittedTxWhenStarted() );

        long result = inner.commit( representation, locks );

        pusher.committed( result, representation.getAuthorId() );

        return result;
    }
}
