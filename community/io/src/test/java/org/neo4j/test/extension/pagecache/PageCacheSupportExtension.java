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
package org.neo4j.test.extension.pagecache;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import java.util.Optional;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.FileSystemExtension;
import org.neo4j.test.extension.StatefullFieldExtension;
import org.neo4j.test.rule.PageCacheConfig;
import org.neo4j.test.rule.PageCacheRule;

import static org.neo4j.test.rule.PageCacheConfig.config;

public class PageCacheSupportExtension extends StatefullFieldExtension<PageCache>
{
    private static final String PAGE_CACHE = "pageCache";
    private static final Namespace PAGE_CACHE_NAMESPACE = Namespace.create( PAGE_CACHE );

    private final PageCacheConfig config;

    public PageCacheSupportExtension()
    {
        this( config() );
    }

    public PageCacheSupportExtension( PageCacheConfig config )
    {
        this.config = config;
    }

    @Override
    protected String getFieldKey()
    {
        return PAGE_CACHE;
    }

    @Override
    protected Class<PageCache> getFieldType()
    {
        return PageCache.class;
    }

    @Override
    protected PageCache createField( ExtensionContext extensionContext )
    {
        Store fileSystemStore = getStore( extensionContext, FileSystemExtension.FILE_SYSTEM_NAMESPACE );
        FileSystemAbstraction contextFileSystem = fileSystemStore.get( FileSystemExtension.FILE_SYSTEM, FileSystemAbstraction.class );
        FileSystemAbstraction pageCacheFileSystem = Optional.ofNullable( contextFileSystem )
                                                    .orElseGet( DefaultFileSystemAbstraction::new );
        return new PageCacheRule().getPageCache( pageCacheFileSystem, config );
    }

    public PageCache getPageCache( FileSystemAbstraction fs, PageCacheConfig config )
    {
        return new PageCacheRule().getPageCache( fs, config );
    }

    public PageCache getPageCache( FileSystemAbstraction fs )
    {
        return new PageCacheRule().getPageCache( fs, config() );
    }

    @Override
    protected Namespace getNameSpace()
    {
        return PAGE_CACHE_NAMESPACE;
    }

    @Override
    public void afterAll( ExtensionContext context ) throws Exception
    {
        PageCache storedValue = getStoredValue( context );
        if ( storedValue != null )
        {
            storedValue.close();
        }
        super.afterAll( context );
    }
}
