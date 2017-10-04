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
package org.neo4j.kernel.api.index;

import java.io.File;

import org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor;

import static org.neo4j.io.fs.FileUtils.path;

/**
 * Dictates how directory structure looks for a {@link SchemaIndexProvider} and its indexes. Generally there's a
 * {@link #rootDirectory() root directory} which contains all index directories in some shape and form.
 * For getting a directory (which must be a sub-directory to the root directory) for a particular index there's the
 * {@link #directoryForIndex(long)} method.
 *
 * These instances are created from a {@link Factory} which typically gets passed into a {@link SchemaIndexProvider} constructor,
 * which then creates a {@link IndexDirectoryStructure} given its {@link Descriptor}.
 */
public abstract class IndexDirectoryStructure
{
    /**
     * Creates an {@link IndexDirectoryStructure} for a {@link Descriptor} for a {@link SchemaIndexProvider}.
     */
    public interface Factory
    {
        IndexDirectoryStructure forProvider( SchemaIndexProvider.Descriptor descriptor );
    }

    private static class SubDirectoryByIndexId extends IndexDirectoryStructure
    {
        private final File providerRootFolder;

        private SubDirectoryByIndexId( File providerRootFolder )
        {
            this.providerRootFolder = providerRootFolder;
        }

        @Override
        public File rootDirectory()
        {
            return providerRootFolder;
        }

        @Override
        public File directoryForIndex( long indexId )
        {
            return path( providerRootFolder, String.valueOf( indexId ) );
        }
    }

    /**
     * Returns the base schema index directory, i.e.
     *
     * <pre>
     * &lt;db&gt;/schema/index/
     * </pre>
     *
     * @param databaseStoreDir database store directory, i.e. {@code db} in the example above, where e.g. {@code nodestore} lives.
     * @return the base directory of schema indexing.
     */
    static File baseSchemaIndexFolder( File databaseStoreDir )
    {
        return path( databaseStoreDir, "schema", "index" );
    }

    /**
     * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
     * @return {@link Factory} for creating {@link IndexDirectoryStructure} returning directories looking something like:
     *
     * <pre>
     * &lt;db&gt;/schema/index/&lt;providerKey&gt;/&lt;indexId&gt;/
     * </pre>
     */
    public static Factory directoriesByProviderKey( File databaseStoreDir )
    {
        return descriptor -> new SubDirectoryByIndexId(
                path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( descriptor.getKey() ) ) );
    }

    /**
    * @param databaseStoreDir store directory of database, i.e. {@code db} in the example above.
    * @return {@link Factory} for creating {@link IndexDirectoryStructure} returning directories looking something like:
    *
    * <pre>
    * &lt;db&gt;/schema/index/&lt;providerKey&gt;-&lt;providerVersion&gt;/&lt;indexId&gt;/
    * </pre>
    */
    public static Factory directoriesByProvider( File databaseStoreDir )
    {
        return descriptor -> new SubDirectoryByIndexId(
                path( baseSchemaIndexFolder( databaseStoreDir ), fileNameFriendly( descriptor ) ) );
    }

    /**
     * @param directoryStructure existing {@link IndexDirectoryStructure}.
     * @return a {@link Factory} returning an already existing {@link IndexDirectoryStructure}.
     */
    public static Factory given( IndexDirectoryStructure directoryStructure )
    {
        return descriptor -> directoryStructure;
    }

    /**
     * Useful when combining multiple {@link SchemaIndexProvider} into one.
     *
     * @param parentStructure {@link IndexDirectoryStructure} of the parent.
     * @return {@link Factory} creating {@link IndexDirectoryStructure} looking something like:
     *
     * <pre>
     * &lt;db&gt;/schema/index/.../&lt;indexId&gt;/&lt;childProviderKey&gt;-&lt;childProviderVersion&gt;/
     * </pre>
     */
    public static Factory directoriesBySubProvider( IndexDirectoryStructure parentStructure )
    {
        return new Factory()
        {
            @Override
            public IndexDirectoryStructure forProvider( Descriptor descriptor )
            {
                return new IndexDirectoryStructure()
                {
                    @Override
                    public File rootDirectory()
                    {
                        return parentStructure.rootDirectory();
                    }

                    @Override
                    public File directoryForIndex( long indexId )
                    {
                        return path( parentStructure.directoryForIndex( indexId ), fileNameFriendly( descriptor ) );
                    }
                };
            }
        };
    }

    private static String fileNameFriendly( String name )
    {
        return name.replaceAll( "\\+", "_" );
    }

    private static String fileNameFriendly( Descriptor descriptor )
    {
        return fileNameFriendly( descriptor.getKey() + "-" + descriptor.getVersion() );
    }

    private static final IndexDirectoryStructure NO_DIRECTORY_STRUCTURE = new IndexDirectoryStructure()
    {
        @Override
        public File rootDirectory()
        {
            return null; // meaning there's no persistent storage
        }

        @Override
        public File directoryForIndex( long indexId )
        {
            return null; // meaning there's no persistent storage
        }
    };

    /**
     * Useful for some in-memory index providers or similar.
     */
    public static final Factory NONE = descriptor -> NO_DIRECTORY_STRUCTURE;

    /**
     * Returns root directory. Must be parent (one or more steps) to all sub-directories returned from {@link #directoryForIndex(long)}.
     * Returns something equivalent to:
     *
     * <pre>
     * &lt;db&gt;/schema/index/&lt;provider&gt;/
     * </pre>
     *
     * @return {@link File} denoting root directory for this provider.
     * May return {@code null} if there's no root directory, i.e. no persistent storage at all.
     */
    public abstract File rootDirectory();

    /**
     * Returns a sub-directory (somewhere under {@link #rootDirectory()}) for a specific index id, looking something equivalent to:
     *
     * <pre>
     * &lt;db&gt;/schema/index/&lt;provider&gt;/&lt;indexId&gt;/
     * </pre>
     *
     * I.e. the root of the schema indexes for this specific provider.
     *
     * @param indexId index id to return directory for.
     * @return {@link File} denoting directory for the specific {@code indexId} for this provider.
     * May return {@code null} if there's no root directory, i.e. no persistent storage at all.
     */
    public abstract File directoryForIndex( long indexId );
}
