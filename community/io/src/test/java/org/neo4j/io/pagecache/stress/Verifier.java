/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.io.pagecache.stress;

import static org.junit.Assert.assertTrue;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

public class Verifier
{
    public void verify( PagedFile pagedFile, int maxPages, int recordsPerPage, RecordVerifierUpdater recordVerifierUpdater ) throws Exception
    {
        try ( PageCursor cursor = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            for ( int pageNumber = 0; pageNumber < maxPages; pageNumber++ )
            {
                assertTrue( cursor.next() );

                for ( int recordNumber = 0; recordNumber < recordsPerPage; recordNumber++ )
                {
                    recordVerifierUpdater.verifyChecksum( cursor, recordNumber );
                }
            }
        }
    }
}
