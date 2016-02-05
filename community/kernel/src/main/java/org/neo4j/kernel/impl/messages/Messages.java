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
package org.neo4j.kernel.impl.messages;

/**
 * This is a beach head - if you are implementing code that prints human language
 * messages in any way, through errors, warnings, explanations or any other
 * mechanism - please help expand, refactor, move and use this this class to do
 * so.
 *
 * The intention of this is to slowly introduce a proper i18n message API,
 * backed by the standard i18n support in Java.
 */
public interface Messages
{
    Message proc_invalid_return_type_description =
            msg("Procedures must return a Stream of records, where a record is a concrete class " +
                "that you define, with public non-final fields defining the fields in the record. " +
                "If you'd like your procedure to return `%s`, you could define a record class like:\n" +
                "public class Output {\n" +
                "    public %s out;\n" +
                "}\n" +
                "\n" +
                "And then define your procedure as returning `Stream<Output>`.");

    Message proc_static_field_annotated_as_context =
            msg("The field `%s` in the class named `%s` is annotated as a @Context field," +
                "but it is static. @Context fields must be public, non-final and non-static," +
                "because they are reset each time a procedure is invoked.");

    interface Message
    {
        String defaultMessage();
    }

    static String get( Message msg, Object ... args )
    {
        return String.format( msg.defaultMessage(), args );
    }

    static Message msg(String defaultMessage)
    {
        return () -> defaultMessage;
    }
}
