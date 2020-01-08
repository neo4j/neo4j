/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.test;


import org.junit.AssumptionViolatedException;

import java.io.Closeable;
import java.io.File;

public final class AssumptionHelper
{
    private AssumptionHelper()
    {
    }

    /**
     * Removes read permissions for a file and returns a {@link Closeable} that will restore permissions when closed.
     * If for some reason the current user is unable to change permissions the test will be aborted/ignored.
     *
     * @param file File to remove read permissions on.
     * @return A {@link Closeable} that will restore permissions when closed.
     * @throws AssumptionViolatedException if the assumptions fail, e.i. user is unable to change permissions or can
     * still read the file even without permissions.
     */
    @SuppressWarnings( "ResultOfMethodCallIgnored" )
    public static Closeable withoutReadPermissions( File file )
    {
        if ( !file.setReadable( false ) )
        {
            throw new AssumptionViolatedException( "User is unable to change permissions on file " + file.getAbsolutePath() );
        }
        if ( file.canRead() )
        {
            file.setReadable( true );
            throw new AssumptionViolatedException( "User can read unreadable file " + file.getAbsolutePath() );
        }
        return () -> file.setReadable( true );
    }

    /**
     * Removes write permissions for a file and returns a {@link Closeable} that will restore permissions when closed.
     * If for some reason the current user is unable to change permissions the test will be aborted/ignored.
     *
     * @param file File to remove write permissions on.
     * @return A {@link Closeable} that will restore permissions when closed.
     * @throws AssumptionViolatedException if the assumptions fail, e.i. user is unable to change permissions or can
     * still write to the file even without permissions.
     */
    @SuppressWarnings( "ResultOfMethodCallIgnored" )
    public static Closeable withoutWritePermissions( File file )
    {
        if ( !file.setWritable( false ) )
        {
            throw new AssumptionViolatedException( "User is unable to change permissions on file " + file.getAbsolutePath() );
        }
        if ( file.canWrite() )
        {
            file.setWritable( true );
            throw new AssumptionViolatedException( "User can read unreadable file " + file.getAbsolutePath() );
        }
        return () -> file.setWritable( true );
    }
}
