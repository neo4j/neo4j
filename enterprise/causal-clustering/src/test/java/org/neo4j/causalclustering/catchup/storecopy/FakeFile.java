/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

/**
 * A fake file tracks a file, but also several counters and helpers that can be used in tests to invoke desired behaviour
 */
class FakeFile
{
    private File file;
    private String filename;
    private String content;
    private int remainingNoResponse;
    private int remainingFailed;
    private Path relativePath;

    FakeFile( String name, String content )
    {
        setFilename( name );
        this.content = content;
    }

    public void setFilename( String filename )
    {
        this.filename = filename;
        this.file = getRelativePath().resolve( filename ).toFile();
    }

    public void setFile( File file )
    {
        this.filename = file.getName();
        this.file = file;
    }

    private Path getRelativePath()
    {
        return Optional.ofNullable( relativePath ).orElse( new File( "." ).toPath() );
    }

    public File getFile()
    {
        return file;
    }

    public String getFilename()
    {
        return filename;
    }

    public String getContent()
    {
        return content;
    }

    public void setContent( String content )
    {
        this.content = content;
    }

    /**
     * Clear response that the file has failed to copy (safe connection close, communication, ...)
     *
     * @return
     */
    int getRemainingFailed()
    {
        return remainingFailed;
    }

    void setRemainingFailed( int remainingFailed )
    {
        this.remainingFailed = remainingFailed;
    }
}
