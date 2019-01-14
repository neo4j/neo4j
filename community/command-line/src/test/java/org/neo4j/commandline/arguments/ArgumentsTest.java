/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.arguments;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.commandline.admin.IncorrectUsage;

import static org.junit.Assert.assertEquals;

public class ArgumentsTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    private Arguments builder;

    @Before
    public void setup()
    {
        builder = new Arguments();
    }

    @Test
    public void throwsOnUnexpectedLongArgument() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "unrecognized option: 'stacktrace'" );

        builder.withDatabase().parse( new String[]{ "--stacktrace" } );
    }

    @Test
    public void throwsOnUnexpectedLongArgumentWithValue() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "unrecognized option: 'stacktrace'" );

        builder.withDatabase().parse( new String[]{ "--stacktrace=true" } );
    }

    @Test
    public void throwsOnUnexpectedShortArgument() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "unrecognized option: 'f'" );

        builder.withDatabase().parse( new String[]{ "-f" } );
    }

    @Test
    public void throwsOnUnexpectedShortArgumentWithValue() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "unrecognized option: 'f'" );

        builder.withDatabase().parse( new String[]{ "-f=bob" } );
    }

    @Test
    public void throwsOnUnexpectedPositionalArgument() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "unrecognized arguments: 'bob sob'" );

        builder.withDatabase().parse( new String[]{ "bob", "sob" } );
    }

    @Test
    public void throwsOnUnexpectedPositionalArgumentWhenExpectingSome() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "unrecognized arguments: 'three four'" );

        builder.withMandatoryPositionalArgument( 0, "first" )
                .withOptionalPositionalArgument( 1, "second" )
                .parse( new String[]{ "one", "two", "three", "four" } );
    }

    @Test
    public void throwsOnTooFewPositionalArguments() throws Exception
    {
        expected.expect( IncorrectUsage.class );
        expected.expectMessage( "not enough arguments" );

        builder.withMandatoryPositionalArgument( 0, "first" )
                .withOptionalPositionalArgument( 1, "second" )
                .parse( new String[]{} );
    }

    @Test
    public void argumentNoValue() throws Exception
    {
        Arguments args = builder.withArgument( new OptionalBooleanArg( "flag", false, "description" ) );

        args.parse( new String[]{"--flag"} );
        assertEquals( true, args.getBoolean( "flag" ) );

        args.parse( new String[0] );
        assertEquals( false, args.getBoolean( "flag" ) );
    }

    @Test
    public void argumentWithEquals() throws Exception
    {
        Arguments args = builder.withArgument( new OptionalBooleanArg( "flag", false, "description" ) );

        args.parse( new String[]{"--flag=true"} );
        assertEquals( true, args.getBoolean( "flag" ) );

        args.parse( new String[]{"--flag=false"} );
        assertEquals( false, args.getBoolean( "flag" ) );
    }

    @Test
    public void argumentWithSpace() throws Exception
    {
        Arguments args = builder.withArgument( new OptionalBooleanArg( "flag", false, "description" ) );

        args.parse( new String[]{"--flag", "true"} );
        assertEquals( true, args.getBoolean( "flag" ) );

        args.parse( new String[]{"--flag", "false"} );
        assertEquals( false, args.getBoolean( "flag" ) );
    }

    @Test
    public void withDatabaseUsage()
    {
        assertEquals( "[--database=<name>]", builder.withDatabase().usage() );
    }

    @Test
    public void withDatabaseDescription()
    {
        assertEquals( String.format( "How to use%n%noptions:%n" +
                        "  --database=<name>   Name of database. [default:graph.db]" ),
                builder.withDatabase().description( "How to use" ) );
    }

    @Test
    public void withDatabaseToUsage()
    {
        assertEquals( "[--database=<name>] --to=<destination-path>", builder.withDatabase().withTo(
                "Destination file." ).usage() );
    }

    @Test
    public void withDatabaseToDescription()
    {
        assertEquals( String.format( "How to use%n%noptions:%n" +
                        "  --database=<name>         Name of database. [default:graph.db]%n" +
                        "  --to=<destination-path>   Destination file." ),
                builder.withDatabase().withTo( "Destination file." ).description( "How to use" ) );
    }

    @Test
    public void withDatabaseToMultilineDescription()
    {
        assertEquals( String.format( "How to use%n%noptions:%n" +
                        "  --database=<name>         Name of database. [default:graph.db]%n" +
                        "  --to=<destination-path>   This is a long string which should wrap on right%n" +
                        "                            col." ),
                builder.withDatabase()
                        .withTo( "This is a long string which should wrap on right col." )
                        .description( "How to use" ) );
    }

    @Test
    public void longNamesTriggerNewLineFormatting()
    {
        assertEquals( String.format( "How to use%n%noptions:%n" +
                        "  --database=<name>%n" +
                        "      Name of database. [default:graph.db]%n" +
                        "  --to=<destination-path>%n" +
                        "      This is a long string which should not wrap on right col.%n" +
                        "  --loooooooooooooong-variable-name=<loooooooooooooong-variable-value>%n" +
                        "      This is also a long string which should be printed on a new line because%n" +
                        "      of long names." ),
                builder.withDatabase()
                        .withTo( "This is a long string which should not wrap on right col." )
                        .withArgument( new MandatoryNamedArg( "loooooooooooooong-variable-name",
                                "loooooooooooooong-variable-value",
                                "This is also a long string which should be printed on a new line because of long " +
                                        "names." ) )
                        .description( "How to use" ) );
    }

    @Test
    public void descriptionShouldHandleExistingNewlines()
    {
        assertEquals( String.format( "This is the first line%n" +
                        "And this is the second line%n" +
                        "The third line is so long that it requires some wrapping by the code itself%n" +
                        "because as you can see it just keeps going ang going and going and going and%n" +
                        "going and going." ),
                builder.description( String.format(
                        "This is the first line%n" + "And this is the second line%n" +
                                "The third line is so long that it requires some wrapping by the code itself because " +
                                "as you " +
                                "can see it just keeps going ang going and going and going and going and going." ) ) );
    }

    @Test
    public void wrappingHandlesBothKindsOfLineEndingsAndOutputsPlatformDependentOnes()
    {
        assertEquals( String.format( "One with Linux%n" +
                        "One with Windows%n" +
                        "And one which is%n" +
                        "just long and should%n" +
                        "be wrapped by the%n" +
                        "function" ),
                Arguments.wrapText(
                        "One with Linux\n" +
                                "One with Windows\r\n" +
                                "And one which is just long and should be wrapped by the function",
                        20 ) );
    }
}
