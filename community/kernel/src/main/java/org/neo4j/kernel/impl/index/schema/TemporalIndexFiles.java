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
package org.neo4j.kernel.impl.index.schema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.values.storable.ValueGroup;

class TemporalIndexFiles
{
    private final FileSystemAbstraction fs;
    private FileLayout<DateSchemaKey> date;
    private FileLayout<LocalDateTimeSchemaKey> localDateTime;
    private FileLayout<ZonedDateTimeSchemaKey> zonedDateTime;
    private FileLayout<LocalTimeSchemaKey> localTime;
    private FileLayout<ZonedTimeSchemaKey> zonedTime;
    private FileLayout<DurationSchemaKey> duration;

    TemporalIndexFiles( IndexDirectoryStructure directoryStructure, long indexId, SchemaIndexDescriptor descriptor, FileSystemAbstraction fs )
    {
        this.fs = fs;
        File indexDirectory = directoryStructure.directoryForIndex( indexId );
        this.date = new FileLayout<>( new File( indexDirectory, "date" ), new DateLayout(), ValueGroup.DATE );
        this.localTime = new FileLayout<>( new File( indexDirectory, "localTime" ), new LocalTimeLayout(), ValueGroup.LOCAL_TIME );
        this.zonedTime = new FileLayout<>( new File( indexDirectory, "zonedTime" ), new ZonedTimeLayout(), ValueGroup.ZONED_TIME );
        this.localDateTime = new FileLayout<>( new File( indexDirectory, "localDateTime" ), new LocalDateTimeLayout(), ValueGroup.LOCAL_DATE_TIME );
        this.zonedDateTime = new FileLayout<>( new File( indexDirectory, "zonedDateTime" ), new ZonedDateTimeLayout(), ValueGroup.ZONED_DATE_TIME );
        this.duration = new FileLayout<>( new File( indexDirectory, "duration" ), new DurationLayout(), ValueGroup.DURATION );
    }

    Iterable<FileLayout> existing()
    {
        List<FileLayout> existing = new ArrayList<>();
        addIfExists( existing, date );
        addIfExists( existing, localDateTime );
        addIfExists( existing, zonedDateTime );
        addIfExists( existing, localTime );
        addIfExists( existing, zonedTime );
        addIfExists( existing, duration );
        return existing;
    }

    <T> void loadExistingIndexes( TemporalIndexCache<T> indexCache ) throws IOException
    {
        for ( FileLayout fileLayout : existing() )
        {
            indexCache.select( fileLayout.valueGroup );
        }
    }

    FileLayout<DateSchemaKey> date()
    {
        return date;
    }

    FileLayout<LocalTimeSchemaKey> localTime()
    {
        return localTime;
    }

    FileLayout<ZonedTimeSchemaKey> zonedTime()
    {
        return zonedTime;
    }

    FileLayout<LocalDateTimeSchemaKey> localDateTime()
    {
        return localDateTime;
    }

    FileLayout<ZonedDateTimeSchemaKey> zonedDateTime()
    {
        return zonedDateTime;
    }

    FileLayout<DurationSchemaKey> duration()
    {
        return duration;
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

    // .... we will add more explicit accessor methods later

    static class FileLayout<KEY extends NativeSchemaKey<KEY>>
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
