/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import java.io.File;
import java.nio.file.OpenOption;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.Token;

/**
 * Implementation of the label store.
 */
public class LabelTokenStore extends TokenStore<LabelTokenRecord, Token>
{
    public static final String TYPE_DESCRIPTOR = "LabelTokenStore";

    public LabelTokenStore(
            File file,
            Config config,
            IdGeneratorFactory idGeneratorFactory,
            PageCache pageCache,
            LogProvider logProvider,
            DynamicStringStore nameStore,
            RecordFormats recordFormats,
            OpenOption... openOptions )
    {
        super( file, config, IdType.LABEL_TOKEN, idGeneratorFactory, pageCache,
                logProvider, nameStore, TYPE_DESCRIPTOR, new Token.Factory(), recordFormats.labelToken(),
                recordFormats.storeVersion(), openOptions );
    }

    @Override
    public <FAILURE extends Exception> void accept( Processor<FAILURE> processor, LabelTokenRecord record )
            throws FAILURE
    {
        processor.processLabelToken( this, record );
    }
}
