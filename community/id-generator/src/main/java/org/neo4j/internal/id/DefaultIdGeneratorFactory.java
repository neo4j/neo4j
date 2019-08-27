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
package org.neo4j.internal.id;

import java.io.File;
import java.nio.file.OpenOption;
import java.util.EnumMap;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.indexed.IndexedIdGenerator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;

public class DefaultIdGeneratorFactory implements IdGeneratorFactory
{
    private final EnumMap<IdType, IdGenerator> generators = new EnumMap<>( IdType.class );
    protected final FileSystemAbstraction fs;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;

    public DefaultIdGeneratorFactory( FileSystemAbstraction fs, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector )
    {
        this.fs = fs;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
    }

    @Override
    public IdGenerator open( PageCache pageCache, File filename, IdType idType, LongSupplier highIdScanner, long maxId, OpenOption... openOptions )
    {
        IdGenerator generator = instantiate( fs, pageCache, recoveryCleanupWorkCollector, filename, highIdScanner, maxId, idType, openOptions );
        generators.put( idType, generator );
        return generator;
    }

    protected IdGenerator instantiate( FileSystemAbstraction fs, PageCache pageCache, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, File fileName,
            LongSupplier highIdSupplier, long maxValue, IdType idType, OpenOption[] openOptions )
    {
        // highId not used when opening an IndexedIdGenerator
        return new IndexedIdGenerator( pageCache, fileName, recoveryCleanupWorkCollector, idType, highIdSupplier, maxValue, openOptions );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        return generators.get( idType );
    }

    @Override
    public IdGenerator create( PageCache pageCache, File fileName, IdType idType, long highId, boolean throwIfFileExists, long maxId,
            OpenOption... openOptions )
    {
        // For the potential scenario where there's no store (of course this is where this method will be called),
        // but there's a naked id generator, then delete the id generator so that it too starts from a clean state.
        fs.deleteFile( fileName );

        IdGenerator generator = new IndexedIdGenerator( pageCache, fileName, recoveryCleanupWorkCollector, idType, () -> highId, maxId, openOptions );
        generator.checkpoint( UNLIMITED );
        generators.put( idType, generator );
        return generator;
    }

    @Override
    public void visit( Consumer<IdGenerator> visitor )
    {
        generators.values().forEach( visitor );
    }
}
