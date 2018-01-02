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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.UniquenessConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

/**
 * What is a tentative constraint index proxy? Well, the way we build uniqueness constraints is as follows:
 * <ol>
 * <li>Begin a transaction T, which will be the "parent" transaction in this process</li>
 * <li>Execute a mini transaction Tt which will create the index rule to start the index population</li>
 * <li>In T: Sit and wait for the index to be built</li>
 * <li>In T: Create the constraint rule and connect the two</li>
 * </ol>
 *
 * The fully populated index flips to a tentative index. The reason for that is to guard for incoming transactions
 * that gets applied.
 * Such incoming transactions have potentially been verified on another instance with a slightly dated view
 * of the schema and has furthermore made it through some additional checks on this instance since transaction T
 * hasn't yet fully committed. Transaction data gets applied to the neo store first and the index second, so at
 * the point where the applying transaction sees that it violates the constraint it has already modified the store and
 * cannot back out. However the constraint transaction T can. So a violated constraint while
 * in tentative mode does not fail the transaction violating the constraint, but keeps the failure around and will
 * eventually fail T instead.
 */
public class TentativeConstraintIndexProxy extends AbstractDelegatingIndexProxy
{
    private final FlippableIndexProxy flipper;
    private final OnlineIndexProxy target;
    private final Collection<IndexEntryConflictException> failures = new CopyOnWriteArrayList<>();

    public TentativeConstraintIndexProxy( FlippableIndexProxy flipper, OnlineIndexProxy target )
    {
        this.flipper = flipper;
        this.target = target;
    }

    @Override
    public IndexUpdater newUpdater( IndexUpdateMode mode )
    {
        switch( mode )
        {
            case ONLINE:
                return new DelegatingIndexUpdater( target.accessor.newUpdater( mode ) )
                {
                    @Override
                    public void process( NodePropertyUpdate update )
                            throws IOException, IndexEntryConflictException, IndexCapacityExceededException
                    {
                        try
                        {
                            delegate.process( update );
                        }
                        catch ( IndexEntryConflictException conflict )
                        {
                            failures.add( conflict );
                        }
                    }

                    @Override
                    public void close() throws IOException, IndexEntryConflictException, IndexCapacityExceededException
                    {
                        try
                        {
                            delegate.close();
                        }
                        catch ( IndexEntryConflictException conflict )
                        {
                            failures.add( conflict );
                        }
                    }
                };

            case BATCHED:
                return super.newUpdater( mode );

            default:
                throw new ThisShouldNotHappenError( "Stefan", "Unsupported IndexUpdateMode" );

        }
    }

    @Override
    public InternalIndexState getState()
    {
        return failures.isEmpty() ? InternalIndexState.POPULATING : InternalIndexState.FAILED;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[target:" + target + "]";
    }

    @Override
    public IndexReader newReader() throws IndexNotFoundKernelException
    {
        throw new IndexNotFoundKernelException( getDescriptor() + " is still populating" );
    }

    @Override
    protected IndexProxy getDelegate()
    {
        return target;
    }

    @Override
    public void validate() throws ConstraintVerificationFailedKernelException
    {
        if ( !failures.isEmpty() )
        {
            IndexDescriptor descriptor = getDescriptor();
            throw new UniquenessConstraintVerificationFailedKernelException(
                    new UniquenessConstraint( descriptor.getLabelId(), descriptor.getPropertyKeyId() ),
                    new HashSet<>( failures ) );
        }
    }

    @Override
    public void activate()
    {
        if ( failures.isEmpty() )
        {
            flipper.flipTo( target );
        }
        else
        {
            throw new IllegalStateException(
                    "Trying to activate failed index, should have checked the failures earlier..." );
        }
    }
}
