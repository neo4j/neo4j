/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.id;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Predicate;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.api.KernelTransactionsSnapshot;

/**
 * Wraps {@link IdGenerator} for those that have {@link IdType#allowAggressiveReuse() aggressive id reuse}
 * so that ids can be {@link IdGenerator#freeId(long) freed} at safe points in time, after all transactions
 * which were active at the time of freeing, have been closed.
 */
public class BufferingIdGeneratorFactory extends IdGeneratorFactory.Delegate
{
    private final Map<IdType, BufferingIdGenerator> overriddenIdGenerators = new HashMap<>();
    private Supplier<KernelTransactionsSnapshot> boundaries;
    private Predicate<KernelTransactionsSnapshot> safeThreshold;

    public BufferingIdGeneratorFactory( IdGeneratorFactory delegate )
    {
        super( delegate );
    }

    public void initialize( Supplier<KernelTransactionsSnapshot> boundaries, final IdReuseEligibility eligibleForReuse )
    {
        this.boundaries = boundaries;
        this.safeThreshold = new Predicate<KernelTransactionsSnapshot>()
        {
            @Override
            public boolean test( KernelTransactionsSnapshot snapshot )
            {
                return snapshot.allClosed() && eligibleForReuse.isEligible( snapshot );
            }
        };
        for ( BufferingIdGenerator generator : overriddenIdGenerators.values() )
        {
            if ( generator != null )
            {
                generator.initialize( boundaries, safeThreshold );
            }
        }
    }

    @Override
    public IdGenerator open( File filename, int grabSize, IdType idType, long highId )
    {
        IdGenerator generator = super.open( filename, grabSize, idType, highId );
        if ( idType.allowAggressiveReuse() )
        {
            BufferingIdGenerator bufferingGenerator = new BufferingIdGenerator( generator );

            // If shutdown was CLEAN
            // BufferingIdGeneratorFactory has lifecycle:
            //   - Construct
            //   - open (all store files)
            //   - initialize
            //
            // If Shutdown was UNCLEAN
            // BufferingIdGeneratorFactory has lifecycle:
            //   - Construct
            //   - open (all store files) will fail
            //   - initialize (with all generators being null)
            //   - recovery is performed
            //   - open (all store files) again
            //   - initialize will NOT be called again so...
            //   - call initialize on generators after open
            //   = that is why this if-statement is here
            if ( boundaries != null )
            {
                bufferingGenerator.initialize( boundaries, safeThreshold );
            }
            overriddenIdGenerators.put( idType, bufferingGenerator );
            generator = bufferingGenerator;
        }
        return generator;
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        IdGenerator generator = overriddenIdGenerators.get( idType );
        return generator != null ? generator : super.get( idType );
    }

    public void maintenance()
    {
        for ( BufferingIdGenerator generator : overriddenIdGenerators.values() )
        {
            if ( generator != null )
            {
                generator.maintenance();
            }
        }
    }

    public void clear()
    {
        for ( BufferingIdGenerator generator : overriddenIdGenerators.values() )
        {
            if ( generator != null )
            {
                generator.clear();
            }
        }
    }
}
