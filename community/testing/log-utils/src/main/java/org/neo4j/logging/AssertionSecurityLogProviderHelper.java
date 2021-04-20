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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.logging.log4j.LogConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssertionSecurityLogProviderHelper
{
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final Log4jLogProvider logProvider;

    public AssertionSecurityLogProviderHelper()
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

    public void assertContainsMessage( Level level, String message, Integer index )
    {
        String[] logLines = outContent.toString().split( System.lineSeparator() );
        Assertions.assertThat( logLines[index] ).contains( message );
        Assertions.assertThat( logLines[index] ).contains( level.toString() );
    }

    public void assertContainsMessage( Level level, String message )
    {
        Assertions.assertThat( outContent.toString() ).contains( message );
        Assertions.assertThat( outContent.toString() ).contains( level.toString() );
    }

    public void assertContainsMessage( Level level, String format, Object... arguments )
    {
        assertContainsMessage( level, String.format( format, arguments ) );
    }

    public void assertDoesNotContainsMessage( String message )
    {
        Assertions.assertThat( outContent.toString() ).doesNotContain( message );
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
        void contains( LogLineContent... logLines );
    }

    private static class JsonContentValidator implements ContentValidator
    {
        private final String[] contentLines;

        JsonContentValidator( String[] contentLines )
        {
            this.contentLines = contentLines;
        }

        @Override
        public void contains( LogLineContent... logLines )
        {
            try
            {
                assertThat( contentLines.length ).isEqualTo( logLines.length );
                for ( int i = 0; i < logLines.length; i++ )
                {
                    LogLineContent expected = logLines[i];
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, String> map = mapper.readValue( contentLines[i], new TypeReference<>()
                    { } );
                    assertLine( expected, map );
                }
            }
            catch ( JsonProcessingException e )
            {
                throw new RuntimeException( e );
            }
        }

        private void assertLine( LogLineContent expected, Map<String,String> map )
        {
            assertEquals( expected.expectedLevel, map.get( "level" ), "'level' mismatch" );
            assertEquals( expected.expectedSource, map.get( "source" ), "'source' mismatch"  );
            assertEquals( expected.expectedUser, map.get( "username" ), "'user' mismatch"  );
            assertEquals( expected.expectedMessage, map.get( "message" ), "'message' mismatch"  );
        }
    }

    private static class LoggerContentValidator implements ContentValidator
    {
        private final String[] contentLines;

        private static final Pattern LOGGER_LINE_PARSER = Pattern.compile(
                "^(?<time>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}[+-]\\d{4}) " +
                "(?<level>\\w{4,5})\\s{1,2}" +
                "(?<source>embedded-session\\t|bolt-session[^>]*>|server-session(?:\\t[^\\t]*){3})\\t" +
                "\\[(?<user>[^\\s]+)] : " +
                "(?<message>.+?)");

        private LoggerContentValidator( String[] contentLines )
        {
            this.contentLines = contentLines;
        }

        @Override
        public void contains( LogLineContent... logLines )
        {
            assertThat( logLines.length ).isEqualTo( contentLines.length );
            for ( int i = 0; i < logLines.length; i++ )
            {
                assertLine( contentLines[i], logLines[i] );
            }
        }

        private void assertLine( String contentLine, LogLineContent expected )
        {
            Matcher matcher = LOGGER_LINE_PARSER.matcher( contentLine );
            assertTrue( matcher.matches() );
            assertEquals( expected.expectedLevel, matcher.group( "level" ), "'level' mismatch" );
            assertEquals( expected.expectedSource, matcher.group("source"), "'source' mismatch"  );
            assertEquals( expected.expectedUser, matcher.group("user"), "'user' mismatch"  );
            assertEquals( expected.expectedMessage, matcher.group("message"), "'message' mismatch"  );
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
    }
}
