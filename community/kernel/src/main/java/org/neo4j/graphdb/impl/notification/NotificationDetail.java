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
package org.neo4j.graphdb.impl.notification;

import java.util.List;
import java.util.Set;

public interface NotificationDetail
{
    String name();

    String value();

    final class Factory
    {
        public static NotificationDetail index( final String labelName, final String propertyKeyName )
        {
            return createNotificationDetail( "hinted index",
                    String.format( "index on :%s(%s)", labelName, propertyKeyName ), true );
        }

        public static NotificationDetail label( final String labelName )
        {
            return createNotificationDetail( "the missing label name is", labelName, true );
        }

        public static NotificationDetail relationshipType( final String relType )
        {
            return createNotificationDetail( "the missing relationship type is", relType, true );
        }

        public static NotificationDetail propertyName( final String name )
        {
            return createNotificationDetail( "the missing property name is", name, true );
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

        private static NotificationDetail createNotificationDetail( Set<String> elements, String singularTerm,
                String pluralTerm )
        {
            StringBuilder builder = new StringBuilder();
            builder.append( "(" );
            String separator = "";
            for ( String element : elements )
            {
                builder.append( separator );
                builder.append( element );
                separator = ", ";
            }
            builder.append( ")" );
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
    }
}
