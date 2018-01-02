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
package org.neo4j.kernel.impl.store;

import java.io.File;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.RelationshipTypeToken;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.logging.LogProvider;

/**
 * Implementation of the relationship type store. Uses a dynamic store to store
 * relationship type names.
 */
public class RelationshipTypeTokenStore extends TokenStore<RelationshipTypeTokenRecord, RelationshipTypeToken>
{
    public static final String TYPE_DESCRIPTOR = "RelationshipTypeStore";
    public static final int RECORD_SIZE = 1/*inUse*/ + 4/*nameId*/;

    public RelationshipTypeTokenStore(
            File fileName,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicStringStore nameStore )
    {
        super( fileName, config, IdType.RELATIONSHIP_TYPE_TOKEN, idGeneratorFactory, pageCache, logProvider, nameStore,
                new RelationshipTypeToken.Factory() );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, RelationshipTypeTokenRecord record )
            throws FAILURE
    {
        processor.processRelationshipTypeToken( this, record );
    }

    @Override
    protected RelationshipTypeTokenRecord newRecord( int id )
    {
        return new RelationshipTypeTokenRecord( id );
    }

    @Override
    public int getRecordSize()
    {
        return RECORD_SIZE;
    }

    @Override
    public String getTypeDescriptor()
    {
        return TYPE_DESCRIPTOR;
    }

    @Override
    protected boolean isRecordReserved( PageCursor cursor )
    {
        return cursor.getInt() == Record.RESERVED.intValue();
    }
}
