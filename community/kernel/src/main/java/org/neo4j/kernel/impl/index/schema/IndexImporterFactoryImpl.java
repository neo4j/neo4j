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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.IndexImporter;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

public class IndexImporterFactoryImpl implements IndexImporterFactory
{
    private final Config config;

    public IndexImporterFactoryImpl( Config config )
    {
        this.config = config;
    }

    @Override
    public IndexImporter getImporter( IndexDescriptor index, DatabaseLayout layout, FileSystemAbstraction fs, PageCache pageCache, CursorContext cursorContext )
    {
        return new TokenIndexImporter( index, layout, fs, pageCache, cursorContext, config );
    }
}
