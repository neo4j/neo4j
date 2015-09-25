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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.logging.LogProvider;

/**
 * An abstract representation of a store. A store is a file that contains
 * records. Each record has a fixed size (<CODE>getRecordSize()</CODE>) so
 * the position for a record can be calculated by
 * <CODE>id * getRecordSize()</CODE>.
 * <p>
 * A store has an {@link IdGenerator} managing the records that are free or in
 * use.
 */
public abstract class AbstractStore extends CommonAbstractStore
{
    public AbstractStore(
            File fileName,
            Config conf,
            IdType idType,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            FileSystemAbstraction fileSystemAbstraction,
            LogProvider logProvider,
            StoreVersionMismatchHandler versionMismatchHandler )
    {
        super( fileName, conf, idType, idGeneratorFactory, pageCache, logProvider,
                versionMismatchHandler );
    }

    /**
     * Returns the fixed size of each record in this store.
     *
     * @return The record size
     */
    public abstract int getRecordSize();

    @Override
    protected void readAndVerifyBlockSize() throws IOException
    {
        // record size is fixed for non-dynamic stores, so nothing to do here
    }

    @Override
    protected boolean isInUse( byte inUseByte )
    {
        return (inUseByte & 0x1) == Record.IN_USE.intValue();
    }
}
