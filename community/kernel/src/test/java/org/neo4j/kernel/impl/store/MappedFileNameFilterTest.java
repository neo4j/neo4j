/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class MappedFileNameFilterTest
{
    @Test
    public void checkMappedFiles() throws Exception
    {
        String[] mappedFiles = {
                "neostore",
                "neostore.counts.db.a",
                "neostore.counts.db.b",
                "neostore.labelscanstore.db",
                "neostore.propertystore.db.strings"
        };

        MappedFileNameFilter filter = new MappedFileNameFilter();
        File dir = new File( "." );

        for ( String name : mappedFiles)
        {
            assertTrue( name + " should be mapped", filter.accept( dir, name ) );
        }

    }

    @Test
    public void checkNotMappedFiles() throws Exception
    {
        String[] notMappedFiles = {
                "neostore.transaction.db.0",
                "neostore.id",
                "store_lock",
                "_0.cfs"
        };

        MappedFileNameFilter filter = new MappedFileNameFilter();
        File dir = new File( "." );

        for ( String name : notMappedFiles)
        {
            assertFalse( name + " should not be mapped",filter.accept( dir, name ) );
        }
    }
}
