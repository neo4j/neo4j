/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.messages;

import java.text.MessageFormat;

/**
 * This is a beach head - if you are implementing code that prints human language
 * messages in any way, through errors, warnings, explanations or any other
 * mechanism - please help expand, refactor, move and use this this class to do
 * so.
 *
 * The intention of this is to slowly introduce a proper i18n message API,
 * backed by the standard i18n support in Java.
 *
 * The messages in this file are templates fed to the {@link MessageFormat} formatter.
 * If you are including things like times, dates or other locale-sensitive things,
 * please use the pattern utilities provided by {@link MessageFormat} to format them,
 * since that will make it much easier to internationalize them in the future.
 */
public interface Messages
{
    // Note: Single-quotes, `'` and curly brackets have special meaning
    // to MessageFormat and need to be escaped as "''" and "'{'", respectively.

    // Note: This file is going to become very large. As it does, please use
    // best judgement to split it into appropriate categories and sub-files.

    Message proc_invalid_return_type_description =
            msg("Procedures must return a Stream of records, where a record is a concrete class\n" +
                "that you define, with public non-final fields defining the fields in the record.\n" +
                "If you''d like your procedure to return `{0}`, you could define a record class like:\n" +
                "public class Output '{'\n" +
                "    public {0} out;\n" +
                "'}'\n" +
                "\n" +
                "And then define your procedure as returning `Stream<Output>`.");

    Message proc_static_field_annotated_as_context =
            msg("The field `{0}` in the class named `{1}` is annotated as a @Context field,\n" +
                "but it is static. @Context fields must be public, non-final and non-static,\n" +
                "because they are reset each time a procedure is invoked.");

    Message proc_unmappable_type =
            msg("Don''t know how to map `{0}` to the Neo4j Type System.\n" +
                "Please refer to to the documentation for full details.\n" +
                "For your reference, known types are: {1}");

    Message proc_unmappable_argument_type =
            msg("Argument `{0}` at position {1} in `{2}` with\n" +
                "type `{3}` cannot be converted to a Neo4j type: {4}" );

    Message proc_argument_missing_name =
            msg("Argument at position {0} in method `{1}` is missing an `@{2}` annotation.\n" +
                "Please add the annotation, recompile the class and try again." );

    Message proc_argument_name_empty =
            msg("Argument at position {0} in method `{1}` is annotated with a name,\n" +
                "but the name is empty, please provide a non-empty name for the argument." );

    interface Message
    {
        String defaultMessage();
    }

    // Implementation note:
    // We use this silly-looking indirection of calling Messages#get(..)
    // instead of just calling message.defaultMessage() wherever we need
    // these messages. The reason is the intended direction of this interface,
    // which is that we'd be able to plug i18n lookups in underneath, so if
    // you in the future are running Neo4j with a German locale, you'd get
    // german messages out on the other end.

    /**
     * Get a message, optionally filling in positional arguments.
     * @param msg the message to output
     * @param args optional dynamic parts of the message
     * @return the message with the dynamic parameters inserted
     */
    static String get( Message msg, Object ... args )
    {
        return MessageFormat.format( msg.defaultMessage(), args );
    }

    /** Create a new message */
    static Message msg(String defaultMessage)
    {
        return () -> defaultMessage;
    }
}
