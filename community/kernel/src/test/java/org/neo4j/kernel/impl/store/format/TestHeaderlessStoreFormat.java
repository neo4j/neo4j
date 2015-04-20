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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.standard.FixedSizeRecordStoreFormat;
import org.neo4j.kernel.impl.store.standard.StoreToolkit;

public class TestHeaderlessStoreFormat extends FixedSizeRecordStoreFormat<TestRecord, TestCursor>
{
    private final TestRecordFormat recordFormat;

    public TestHeaderlessStoreFormat()
    {
        super( 8, "HeaderlessFormat", "v0.1.0" );
        this.recordFormat = new TestRecordFormat();
    }

    @Override
    public TestCursor createCursor( PagedFile file, StoreToolkit toolkit, int flags )
    {
        return new TestCursor( file, toolkit, recordFormat, flags );
    }

    @Override
    public RecordFormat<TestRecord> recordFormat()
    {
        return recordFormat;
    }
}
