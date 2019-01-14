/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
