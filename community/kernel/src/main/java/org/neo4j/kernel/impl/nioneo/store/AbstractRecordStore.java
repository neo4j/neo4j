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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.Collection;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.util.Collections.singletonList;

public abstract class AbstractRecordStore<R extends AbstractBaseRecord> extends AbstractStore implements RecordStore<R>
{
    public AbstractRecordStore( File fileName, Config conf, IdType idType, IdGeneratorFactory idGeneratorFactory,
                                WindowPoolFactory windowPoolFactory, FileSystemAbstraction fileSystemAbstraction,
                                StringLogger stringLogger )
    {
        super( fileName, conf, idType, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, stringLogger );
    }

    @Override
    public Collection<R> getRecords( long id )
    {
        return singletonList( getRecord( id ) );
    }

    @Override
    public Long getNextRecordReference( R record )
    {
        return null;
    }
}
