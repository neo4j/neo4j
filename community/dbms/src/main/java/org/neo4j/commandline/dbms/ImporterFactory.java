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
package org.neo4j.commandline.dbms;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.configuration.Config;

class ImporterFactory
{
    Importer getImporterForMode( String mode, Args parsedArgs, Config config, OutsideWorld outsideWorld )
            throws IncorrectUsage, CommandFailed
    {
        Importer importer;
        switch ( mode )
        {
        case "database":
            importer = new DatabaseImporter( parsedArgs, config, outsideWorld );
            break;
        case "csv":
            importer = new CsvImporter( parsedArgs, config, outsideWorld );
            break;
        default:
            throw new CommandFailed( "Invalid mode specified." ); // This won't happen because mode is mandatory.
        }
        return importer;
    }
}
