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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.values.storable.ValueGroup;

class TemporalIndexFiles
{
    private final FileSystemAbstraction fs;
    private FileLayout<DateSchemaKey> dateFileLayout;
    private FileLayout dateTimeFileLayout;
    private FileLayout dateTimeZonedFileLayout;
    private FileLayout timeFileLayout;
    private FileLayout timeZonedFileLayout;
    private FileLayout durationFileLayout;

    TemporalIndexFiles( IndexDirectoryStructure directoryStructure, long indexId, IndexDescriptor descriptor, FileSystemAbstraction fs )
    {
        this.fs = fs;
        File indexDirectory = directoryStructure.directoryForIndex( indexId );
        dateFileLayout = new FileLayout<>( new File( indexDirectory, "date" ), DateLayout.of( descriptor ), ValueGroup.DATE );
    }

    Iterable<FileLayout> existing()
    {
        List<FileLayout> existing = new ArrayList<>();
        addIfExists( existing, dateFileLayout );
        addIfExists( existing, dateTimeFileLayout );
        addIfExists( existing, dateTimeZonedFileLayout );
        addIfExists( existing, timeFileLayout );
        addIfExists( existing, timeZonedFileLayout );
        addIfExists( existing, durationFileLayout );
        return existing;
    }

    <T,E extends Exception> void loadExistingIndexes( TemporalIndexCache<T,E> indexCache ) throws E
    {
        for ( FileLayout fileLayout : existing() )
        {
            indexCache.select( fileLayout.valueGroup );
        }
    }

    private void addIfExists( List<FileLayout> existing, FileLayout fileLayout )
    {
        if ( exists( fileLayout ) )
        {
            existing.add( fileLayout );
        }
    }

    private boolean exists( FileLayout fileLayout )
    {
        return fileLayout != null && fs.fileExists( fileLayout.indexFile );
    }

    public FileLayout<DateSchemaKey> date()
    {
        return dateFileLayout;
    }

    // .... we will add more explicit accessor methods later

    static class FileLayout<KEY extends NativeSchemaKey>
    {
        final File indexFile;
        final Layout<KEY,NativeSchemaValue> layout;
        final ValueGroup valueGroup;

        FileLayout( File indexFile, Layout<KEY,NativeSchemaValue> layout, ValueGroup valueGroup )
        {
            this.indexFile = indexFile;
            this.layout = layout;
            this.valueGroup = valueGroup;
        }
    }
}
