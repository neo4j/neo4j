/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.procedure.builtin.routing;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.database.DatabaseReference;

public final class TestDatabaseAvailabilityChecker implements DatabaseAvailabilityChecker
{

    private final Map<DatabaseReference.Internal,Boolean> lookups;

    public TestDatabaseAvailabilityChecker()
    {
        this.lookups = new HashMap<>();
    }

    private TestDatabaseAvailabilityChecker( Map<DatabaseReference.Internal,Boolean> lookups )
    {
        this.lookups = lookups;
    }

    public TestDatabaseAvailabilityChecker withDatabase( DatabaseReference.Internal ref, boolean available )
    {
        var newLookups = new HashMap<>( this.lookups );
        newLookups.put( ref, available );
        return new TestDatabaseAvailabilityChecker( newLookups );
    }

    @Override
    public boolean isAvailable( DatabaseReference.Internal databaseReference )
    {
        return this.lookups.getOrDefault( databaseReference, false );
    }

    @Override
    public boolean isPresent( DatabaseReference.Internal databaseReference )
    {
        return this.lookups.containsKey( databaseReference );
    }
}
