/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.transactional.error;

import java.io.File;
import java.util.TreeMap;

import org.neo4j.kernel.impl.util.FileUtils;

/**
 * Generates Asciidoc for {@link Status}.
 *
 * [options="header", cols=">s,^", width="100%"]
 * ===================
 * Status Code                                    |Description
 * Neo.SomeClassification.SomeCategory.SomeTitle  |Some description
 * ===================
 */
public class ErrorDocumentationGenerator
{
    public static void main( String[] args ) throws Exception
    {
        File baseDir = null;
        if(args.length == 1)
        {
            baseDir = new File(args[0]);

        } else
        {
            System.out.println("Usage: ErrorDocumentationGenerator [output folder]");
            System.exit(0);
        }

        ErrorDocumentationGenerator generator = new ErrorDocumentationGenerator();

        File classificationFile = new File( baseDir, "status-code-classifications.asccidoc" );
        System.out.println("Saving status code classification docs in '" + classificationFile.getAbsolutePath() + "'.");
        FileUtils.writeToFile( classificationFile, generator.generateClassificationDocs(), false );

        File statusCodeFile = new File( baseDir, "status-code-codes.asccidoc" );
        System.out.println("Saving status code statuses docs in '" + statusCodeFile.getAbsolutePath() + "'.");
        FileUtils.writeToFile( statusCodeFile, generator.generateStatusCodeDocs(), false );
    }

    public String generateClassificationDocs()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "[options=\"header\", cols=\">s,^,^\", width=\"100%\"]\n" );
        sb.append( "===================\n" );
        sb.append( "Classification |Description |Effect on transaction\n" );

        for ( Status.Classification classification : Status.Classification.class.getEnumConstants() )
        {
            sb.append( classificationAsRow( classification ) );
        }
        sb.append( "===================\n" );

        return sb.toString();
    }

    private String classificationAsRow( Status.Classification classification )
    {
        String description = classification.description().length() > 0
                ? classification.description()
                : "No description available.";
        String txEffect = classification.rollbackTransaction() ? "Rollback" : "None";
        return classification.name() + " |" + description + " |" + txEffect + "\n";
    }

    public String generateStatusCodeDocs()
    {
        TreeMap<String, Status.Code> sortedStatuses = sortedStatusCodes();

        StringBuilder sb = new StringBuilder();

        sb.append( "[options=\"header\", cols=\">s,^\", width=\"100%\"]\n" );
        sb.append( "===================\n" );
        sb.append( "Status Code |Description\n" );

        for ( String code : sortedStatuses.keySet() )
        {
            sb.append(codeAsTableRow(sortedStatuses.get(code)));
        }

        sb.append( "===================\n" );

        return sb.toString();
    }

    private String codeAsTableRow( Status.Code code )
    {
        String description = code.description().length() > 0 ? code.description() : "No description available.";
        return code.serialize() + " |" + description + "\n";
    }

    private TreeMap<String, Status.Code> sortedStatusCodes()
    {
        TreeMap<String, Status.Code> sortedStatuses = new TreeMap<>();
        for ( Status status : Status.Code.all() )
        {
            sortedStatuses.put( status.code().serialize(), status.code() );
        }
        return sortedStatuses;
    }
}