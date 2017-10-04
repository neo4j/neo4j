/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.licensecheck;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Calendar;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LicenseYearTest
{

    private static final String NOTICE_GPL_FILE = "notice-gpl-prefix.txt";
    private static final String NOTICE_AGPL_FILE = "notice-agpl-prefix.txt";

    @Test
    public void testNoticePrefixYearGPL() throws FileNotFoundException
    {
        if ( System.getProperty( "ignoreNoticeYear" ).isEmpty() )
        {
            URL resource = getClass().getClassLoader().getResource( NOTICE_GPL_FILE );
            File gplFile = new File( resource.getFile() );

            checkYearInFile( gplFile );
        }
    }

    @Test
    public void testNoticePrefixYearAGPL() throws FileNotFoundException
    {
        if ( System.getProperty( "ignoreNoticeYear" ).isEmpty() )
        {
            URL resource = getClass().getClassLoader().getResource( NOTICE_AGPL_FILE );
            File gplFile = new File( resource.getFile() );

            checkYearInFile( gplFile );
        }
    }

    private void checkYearInFile( File gplFile ) throws FileNotFoundException
    {
        try ( Scanner scanner = new Scanner( gplFile ) )
        {
            scanner.nextLine(); // skip first line
            String yearLine = scanner.nextLine();

            Pattern p = Pattern.compile( "Copyright \u00c2??© 2002-(\\d\\d\\d\\d) Network Engine for Objects in Lund AB" );

            Matcher m = p.matcher( yearLine );
            if ( m.find() )
            {
                String yearInFile = m.group( 1 );

                int realYear = Calendar.getInstance().get( Calendar.YEAR );

                assertThat( "The year field in the NOTICE file header template needs to be updated. " +
                            "If you are building an old version of Neo4j, and/or do not care about the copyright " +
                            "year of its modules, set the maven parameter `-DignoreNoticeYear`",
                        Integer.parseInt( yearInFile ), equalTo( realYear ) );
            }
            else
            {
                fail( "No year found in NOTICE header!" );
            }
        }
    }
}
