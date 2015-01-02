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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

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
                    public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
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
                    public void close() throws IOException, IndexEntryConflictException
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

            case RECOVERY:
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
        Iterator<IndexEntryConflictException> iterator = failures.iterator();
        if ( iterator.hasNext() )
        {
            Set<ConstraintVerificationFailedKernelException.Evidence> evidence = new HashSet<>();
            do
            {
                evidence.add( new ConstraintVerificationFailedKernelException.Evidence( iterator.next() ) );
            } while ( iterator.hasNext() );
            IndexDescriptor descriptor = getDescriptor();
            throw new ConstraintVerificationFailedKernelException(
                    new UniquenessConstraint( descriptor.getLabelId(), descriptor.getPropertyKeyId() ), evidence );
        }
    }

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
