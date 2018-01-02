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
package org.neo4j.kernel.impl.storemigration.legacystore;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LegacyStoreVersionCheckTest
{
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldReportMissingFileNotHavingSpecifiedVersion()
    {
        // given
        File missingFile = new File( "/you/will/never/find/me" );
        LegacyStoreVersionCheck storeVersionCheck = new LegacyStoreVersionCheck( new EphemeralFileSystemAbstraction() );

        // then
        assertFalse( storeVersionCheck.hasVersion( missingFile, "version", false ).outcome.isSuccessful() );
    }

    @Test
    public void shouldNotReportMissingOptionalFileNotHaveSpecifiedVersion()
    {
        // given
        File missingFile = new File( "/you/will/never/find/me" );
        LegacyStoreVersionCheck storeVersionCheck = new LegacyStoreVersionCheck( new EphemeralFileSystemAbstraction() );

        // then
        assertTrue( storeVersionCheck.hasVersion( missingFile, "version", true ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportShortFileDoesNotHaveSpecifiedVersion() throws IOException
    {
        // given
        File shortFile = fileContaining( fs.get(), "a" );

        LegacyStoreVersionCheck storeVersionCheck = new LegacyStoreVersionCheck( fs.get() );

        // then
        assertFalse( storeVersionCheck.hasVersion( shortFile, "version", false ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithIncorrectVersion() throws IOException
    {
        // given
        File shortFile = fileContaining( fs.get(), "versionWhichIsIncorrect" );

        LegacyStoreVersionCheck storeVersionCheck = new LegacyStoreVersionCheck( fs.get() );

        // then
        assertFalse( storeVersionCheck.hasVersion( shortFile, "correctVersion 1", false ).outcome.isSuccessful() );
    }

    @Test
    public void shouldReportFileWithCorrectVersion() throws IOException
    {
        // given
        File shortFile = fileContaining( fs.get(), "correctVersion 1" );

        LegacyStoreVersionCheck storeVersionCheck = new LegacyStoreVersionCheck( fs.get() );

        // then
        assertTrue( storeVersionCheck.hasVersion( shortFile, "correctVersion 1", false ).outcome.isSuccessful() );
    }

    private File fileContaining( EphemeralFileSystemAbstraction fs, String content ) throws IOException
    {
        File shortFile = new File( "shortFile" );
        try ( OutputStream outputStream = fs.openAsOutputStream( shortFile, true ) )
        {
            outputStream.write( content.getBytes() );
        }
        return shortFile;
    }
}
