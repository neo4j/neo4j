/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SecurityLogHelper
{
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final Log4jLogProvider logProvider;

    public SecurityLogHelper()
    {
        logProvider = new Log4jLogProvider( LogConfig.createBuilder( outContent, Level.INFO )
                                                     .withFormat( FormattedLogFormat.STANDARD_FORMAT )
                                                     .withCategory( false )
                                                     .build() );
    }

    public Log4jLogProvider getLogProvider()
    {
        return logProvider;
    }

    public void assertDoesNotContainsMessage( String message )
    {
        Assertions.assertThat( outContent.toString() ).doesNotContain( message );
    }

    public ContentValidator assertLog( FormattedLogFormat format )
    {
        String[] contentLines = outContent.toString().split( System.lineSeparator() );
        return assertLog( format, contentLines );
    }

    public static ContentValidator assertLog( FormattedLogFormat format, String[] content )
    {
        switch ( format )
        {
        case STANDARD_FORMAT:
            return new LoggerContentValidator( content );
        case JSON_FORMAT:
            return new JsonContentValidator( content );
        default:
        }
        throw new AssertionError();
    }

    public interface ContentValidator
    {
        void containsOnly( LogLineContent... logLines );

        void containsOrdered( LogLineContent... logLines );
    }

    private static class JsonContentValidator implements ContentValidator
    {
        private final String[] contentLines;

        JsonContentValidator( String[] contentLines )
        {
            this.contentLines = contentLines;
        }

        @Override
        public void containsOnly( LogLineContent... logLines )
        {
            try
            {
                assertThat( contentLines.length ).isEqualTo( logLines.length );
                for ( int i = 0; i < logLines.length; i++ )
                {
                    LogLineContent expected = logLines[i];
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String,String> map = mapper.readValue( contentLines[i], new TypeReference<>()
                    {
                    } );
                    assertLine( expected, map );
                }
            }
            catch ( JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public void containsOrdered( LogLineContent... logLines )
        {
            try
            {
                assertThat( contentLines.length ).isGreaterThanOrEqualTo( logLines.length );

                int j = 0;
                for ( LogLineContent expected : logLines )
                {
                    boolean found = false;
                    for ( ; j < contentLines.length; j++ )
                    {
                        ObjectMapper mapper = new ObjectMapper();
                        Map<String,String> map = mapper.readValue( contentLines[j], new TypeReference<>()
                        { } );
                        if ( equalLine( expected, map ) )
                        {
                            found = true;
                            break;
                        }
                    }
                    assertTrue( found, String.format( "Did not find line:  %s", expected ) );
                }
            }
            catch ( JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }

        private boolean equalLine( LogLineContent expected, Map<String,String> map )
        {
            return Objects.equals( expected.expectedLevel, map.get( "level" ) ) &&
                   Objects.equals( expected.expectedSource, map.get( "source" ) ) &&
                   Objects.equals( expected.expectedUser, map.get( "username" ) ) &&
                   Objects.equals( expected.expectedMessage, map.get( "message" ) );
        }

        private void assertLine( LogLineContent expected, Map<String,String> map )
        {
            assertEquals( expected.expectedLevel, map.get( "level" ), "'level' mismatch" );
            assertEquals( expected.expectedSource, map.get( "source" ), "'source' mismatch" );
            assertEquals( expected.expectedUser, map.get( "username" ), "'user' mismatch" );
            assertEquals( expected.expectedMessage, map.get( "message" ), "'message' mismatch" );
        }
    }

    private static class LoggerContentValidator implements ContentValidator
    {
        private final String[] contentLines;

        private static final Pattern LOGGER_LINE_PARSER = Pattern.compile(
                "^(?<time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{4}) " +
                "(?<level>\\w{4,5})\\s{1,2}" +
                "((?<source>embedded-session\\t|bolt-session[^>]*>|server-session(?:\\t[^\\t]*){3})\\t)?" +
                "\\[(?<user>[^\\s]+)?] : " +
                "(?<message>.+?)" );

        private LoggerContentValidator( String[] contentLines )
        {
            this.contentLines = contentLines;
        }

        @Override
        public void containsOnly( LogLineContent... logLines )
        {
            assertThat( logLines.length ).isEqualTo( contentLines.length );
            for ( int i = 0; i < logLines.length; i++ )
            {
                assertLine( contentLines[i], logLines[i] );
            }
        }

        @Override
        public void containsOrdered( LogLineContent... logLines )
        {
            assertThat( contentLines.length ).isGreaterThanOrEqualTo( logLines.length );

            int j = 0;
            for ( LogLineContent expected : logLines )
            {
                boolean found = false;
                for ( ; j < contentLines.length; j++ )
                {
                    if ( equalLine( contentLines[j], expected ) )
                    {
                        found = true;
                        break;
                    }
                }
                assertTrue( found, String.format( "Did not find line:  %s %nin %n%s", expected, Arrays.toString( contentLines ) ) );
            }
        }

        private boolean equalLine( String contentLine, LogLineContent expected )
        {
            Matcher matcher = LOGGER_LINE_PARSER.matcher( contentLine );
            return matcher.matches() &&
                   Objects.equals( expected.expectedLevel, matcher.group( "level" ) ) &&
                   Objects.equals( expected.expectedSource, matcher.group( "source" ) ) &&
                   Objects.equals( expected.expectedUser, matcher.group( "user" ) ) &&
                   Objects.equals( expected.expectedMessage, matcher.group( "message" ) );
        }

        private void assertLine( String contentLine, LogLineContent expected )
        {
            Matcher matcher = LOGGER_LINE_PARSER.matcher( contentLine );
            assertTrue( matcher.matches() );
            assertEquals( expected.expectedLevel, matcher.group( "level" ), "'level' mismatch" );
            assertEquals( expected.expectedSource, matcher.group( "source" ), "'source' mismatch" );
            assertEquals( expected.expectedUser, matcher.group( "user" ), "'user' mismatch" );
            assertEquals( expected.expectedMessage, matcher.group( "message" ), "'message' mismatch" );
        }
    }

    public static LogLineContent line()
    {
        return new LogLineContent();
    }

    public static class LogLineContent
    {
        private String expectedLevel;
        private String expectedSource;
        private String expectedUser;
        private String expectedMessage;

        public LogLineContent level( Level level )
        {
            this.expectedLevel = level.toString();
            return this;
        }

        public LogLineContent source( String source )
        {
            this.expectedSource = source;
            return this;
        }

        public LogLineContent user( String username )
        {
            this.expectedUser = username;
            return this;
        }

        public LogLineContent message( String message )
        {
            this.expectedMessage = message;
            return this;
        }

        @Override
        public String toString()
        {
            return "LogLineContent{" +
                   "expectedLevel='" + expectedLevel + '\'' +
                   ", expectedSource='" + expectedSource + '\'' +
                   ", expectedUser='" + expectedUser + '\'' +
                   ", expectedMessage='" + expectedMessage + '\'' +
                   '}';
        }
    }
}
