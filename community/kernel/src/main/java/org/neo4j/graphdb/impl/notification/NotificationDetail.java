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
package org.neo4j.graphdb.impl.notification;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public interface NotificationDetail
{
    String name();

    String value();

    final class Factory
    {
        private Factory()
        {
        }

        public static NotificationDetail deprecatedName( final String oldName, final String newName )
        {
            return createDeprecationNotificationDetail( oldName, newName );
        }

        public static NotificationDetail index( final String labelName, final String... propertyKeyNames )
        {
            return createNotificationDetail( "hinted index",
                    String.format( "index on :%s(%s)", labelName,
                            Arrays.stream( propertyKeyNames ).collect( Collectors.joining( "," ) ) ), true );
        }

        public static NotificationDetail suboptimalIndex( final String labelName, final String... propertyKeyNames )
        {
            return createNotificationDetail( "index",
                    String.format( "index on :%s(%s)", labelName,
                            Arrays.stream( propertyKeyNames ).collect( Collectors.joining( "," ) ) ), true );
        }

        public static NotificationDetail label( final String labelName )
        {
            return createNotificationDetail( "the missing label name", labelName, true );
        }

        public static NotificationDetail relationshipType( final String relType )
        {
            return createNotificationDetail( "the missing relationship type", relType, true );
        }

        public static NotificationDetail procedureWarning( final String procedure, final String warning )
        {
            return createProcedureWarningNotificationDetail( procedure, warning );
        }

        public static NotificationDetail propertyName( final String name )
        {
            return createNotificationDetail( "the missing property name", name, true );
        }

        public static NotificationDetail joinKey( List<String> identifiers )
        {
            boolean singular = identifiers.size() == 1;
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for ( String identifier : identifiers )
            {
                if ( first )
                {
                    first = false;
                }
                else
                {
                    builder.append( ", " );
                }
                builder.append( identifier );
            }
            return createNotificationDetail(
                singular ? "hinted join key identifier" : "hinted join key identifiers",
                builder.toString(),
                singular
            );
        }

        public static NotificationDetail cartesianProduct( Set<String> identifiers )
        {
            return createNotificationDetail( identifiers, "identifier", "identifiers" );
        }

        public static NotificationDetail indexSeekOrScan( Set<String> labels )
        {
            return createNotificationDetail( labels, "indexed label", "indexed labels" );
        }

        public static NotificationDetail message( String name, String message )
        {
            return new NotificationDetail()
            {
                @Override
                public String name()
                {
                    return name;
                }

                @Override
                public String value()
                {
                    return message;
                }

                @Override
                public String toString()
                {
                    return message;
                }
            };
        }

        public static NotificationDetail deprecatedField( final String procedure, final String field )
        {
            return new NotificationDetail()
            {
                @Override
                public String name()
                {
                    return procedure;
                }

                @Override
                public String value()
                {
                    return field;
                }

                @Override
                public String toString()
                {
                    return String.format( "'%s' returned by '%s' is no longer supported.", field, procedure );
                }
            };
        }

        public static NotificationDetail bindingVarLengthRelationship( final String element )
        {
            return new NotificationDetail()
            {
                @Override
                public String name()
                {
                    return element;
                }

                @Override
                public String value()
                {
                    return String.format(
                            "Binding a variable length relationship pattern to a variable ('%s') is deprecated and "
                                    + "will be unsupported in a future version. The recommended way is to bind the "
                                    + "whole path to a variable, then extract the relationships:%n"
                                    + "\tMATCH p = (...)-[...]-(...)%n"
                                    + "\tWITH *, relationships(p) AS %s",
                            element, element );
                }

                @Override
                public String toString()
                {
                    return value();
                }
            };
        }

        private static NotificationDetail createNotificationDetail( Set<String> elements, String singularTerm,
                String pluralTerm )
        {
            StringBuilder builder = new StringBuilder();
            builder.append( '(' );
            String separator = "";
            for ( String element : elements )
            {
                builder.append( separator );
                builder.append( element );
                separator = ", ";
            }
            builder.append( ')' );
            boolean singular = elements.size() == 1;
            return createNotificationDetail( singular ? singularTerm : pluralTerm, builder.toString(), singular );
        }

        private static NotificationDetail createNotificationDetail( final String name, final String value,
                final boolean singular )
        {
            return new NotificationDetail()
            {
                @Override
                public String name()
                {
                    return name;
                }

                @Override
                public String value()
                {
                    return value;
                }

                @Override
                public String toString()
                {
                    return String.format( "%s %s %s", name, singular ? "is:" : "are:", value );
                }
            };
        }

        private static NotificationDetail createDeprecationNotificationDetail( final String oldName, final String newName )
        {
            return new NotificationDetail()
            {
                @Override
                public String name()
                {
                    return oldName;
                }

                @Override
                public String value()
                {
                    return newName;
                }

                @Override
                public String toString()
                {
                    if ( newName == null || newName.trim().isEmpty() )
                    {
                        return String.format( "'%s' is no longer supported", oldName );
                    }
                    else
                    {
                        return String.format( "'%s' has been replaced by '%s'", oldName, newName );
                    }
                }
            };
        }

        private static NotificationDetail createProcedureWarningNotificationDetail( String procedure, String warning )
        {
            return new NotificationDetail()
            {
                @Override
                public String name()
                {
                    return procedure;
                }

                @Override
                public String value()
                {
                    return warning;
                }

                @Override
                public String toString()
                {
                    return String.format( warning, procedure );
                }
            };
        }
    }
}
