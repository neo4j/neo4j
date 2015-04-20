/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.junit.Test;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StoreVersionCheckTest
{
    @Test
    public void shouldReportMissingFileDoesNotHaveSpecifiedVersion()
    {
        // given
        File missingFile = new File("/you/will/never/find/me");
        StoreVersionCheck storeVersionCheck = new StoreVersionCheck(new EphemeralFileSystemAbstraction());

        // then
        assertFalse( storeVersionCheck.hasVersion( missingFile, "version" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportShortFileDoesNotHaveSpecifiedVersion() throws IOException
    {
        // given
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        File shortFile = fileContaining( fs, "a" );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( fs );

        // then
        assertFalse( storeVersionCheck.hasVersion( shortFile, "version" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithIncorrectVersion() throws IOException
    {
        // given
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        File shortFile = fileContaining( fs, "versionWhichIsIncorrect" );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( fs );

        // then
        assertFalse( storeVersionCheck.hasVersion( shortFile, "correctVersion 1" ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
        File shortFile = fileContaining( fs, "correctVersion 1" );

        StoreVersionCheck storeVersionCheck = new StoreVersionCheck( fs );

        // then
        assertTrue( storeVersionCheck.hasVersion( shortFile, "correctVersion 1" ).outcome.isSuccessful() );
    }

    private File fileContaining( EphemeralFileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = new File( "shortFile" );
        OutputStream outputStream = fs.openAsOutputStream( shortFile, true );
        outputStream.write( content.getBytes() );
        outputStream.close();
        return shortFile;
    }
}
