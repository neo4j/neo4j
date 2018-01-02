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
package org.neo4j.commandline.dbms.config;

import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
/**
 * Provides a wrapper around {@link Configuration} with overridden defaults for neo4j-admin import
 * Always trim strings
 * Import emptyQuotedStrings as empty Strings
 * Buffer size is set to 4MB
 */

public class WrappedCsvInputConfigurationForNeo4jAdmin implements Configuration
{
    private Configuration defaults;

    public WrappedCsvInputConfigurationForNeo4jAdmin( Configuration defaults )
    {
        this.defaults = defaults;
    }

    @Override
    public char delimiter()
    {
        return defaults.delimiter();
    }

    @Override
    public char arrayDelimiter()
    {
        return defaults.arrayDelimiter();
    }

    @Override
    public char quotationCharacter()
    {
        return defaults.quotationCharacter();
    }

    @Override
    public int bufferSize()
    {
        return DEFAULT_BUFFER_SIZE_4MB;
    }

    @Override
    public boolean multilineFields()
    {
        return defaults.multilineFields();
    }

    @Override
    public boolean trimStrings()
    {
        return true;
    }

    @Override
    public boolean emptyQuotedStringsAsNull()
    {
        return false;
    }

    @Override
    public boolean legacyStyleQuoting()
    {
        return false;
    }
}
