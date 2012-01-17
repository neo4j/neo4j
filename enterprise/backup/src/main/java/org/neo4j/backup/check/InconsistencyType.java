/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.check;

import java.util.Collection;

import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

public interface InconsistencyType
{
    String message();
    
    boolean isWarning();

    enum ReferenceInconsistency implements InconsistencyType
    {
        INVALID_TYPE_ID( "invalid relationship type id" ),
        TYPE_NOT_IN_USE( "relationship type not in use" ),
        RELATIONSHIP_NOT_IN_USE( "reference to relationship not in use" ),
        RELATIONSHIP_FOR_OTHER_NODE( "reference to relationship that does not reference back" ),
        PROPERTY_NOT_IN_USE( "reference to property not in use" ),
        PROPERTY_FOR_OTHER( "reference to property for other entity" ),
        NEXT_PROPERTY_NOT_IN_USE( "invalid next reference, next record not in use" ),
        PROPERTY_NEXT_WRONG_BACKREFERENCE( "invalid next reference, next record does not reference back" ),
        PREV_PROPERTY_NOT_IN_USE( "invalid prev reference, prev record not in use" ),
        PROPERTY_PREV_WRONG_BACKREFERENCE( "invalid prev reference, prev record does not reference back" ),
        OWNER_NOT_IN_USE( "owning record not in use" ),
        OWNER_DOES_NOT_REFERENCE_BACK( "owning record does not reference back" ),
        REPLACED_PROPERTY( "replacing used property record" ),
        NEXT_DYNAMIC_NOT_IN_USE( "next dynamic record not in use" ),
        NON_FULL_DYNAMIC_WITH_NEXT( "next record set, but length less than maximum" ),
        DYNAMIC_LENGTH_TOO_LARGE( "length larger than maximum for store" ),
        OVERWRITE_USED_DYNAMIC( "overwriting used dynamic record" ),
        UNUSED_TYPE_NAME( "reference to unused type name" ),
        UNUSED_KEY_NAME( "reference to unused key string" ),
        SOURCE_NODE_INVALID( "invalid source node reference" ),
        SOURCE_NODE_NOT_IN_USE( "invalid source node reference, not in use" ),
        TARGET_NODE_INVALID( "invalid target node reference" ),
        TARGET_NODE_NOT_IN_USE( "invalid target node reference, not in use" ),
        TARGET_PREV_NOT_IN_USE( "prev(target) reference to record not used" ),
        TARGET_NO_BACKREF( "relationship first in target chain, but target node does not reference back" ),
        TARGET_PREV_DIFFERENT_CHAIN( "not part of the same chain, invalid prev(target) reference" ),
        SOURCE_PREV_NOT_IN_USE( "prev(target) reference to record not used" ),
        SOURCE_NO_BACKREF( "relationship first in source chain, but source node does not reference back" ),
        SOURCE_PREV_DIFFERENT_CHAIN( "not part of the same chain, invalid prev(target) reference" ),
        TARGET_NEXT_NOT_IN_USE( "prev(target) reference to record not used" ),
        TARGET_NEXT_DIFFERENT_CHAIN( "not part of the same chain, invalid prev(target) reference" ),
        SOURCE_NEXT_NOT_IN_USE( "prev(target) reference to record not used" ),
        SOURCE_NEXT_DIFFERENT_CHAIN( "not part of the same chain, invalid prev(target) reference" ),
        PROPERTY_CHANGED_WITHOUT_OWNER( true, "the property record was changed but did not have an owning node or relationship" ),
        RELATIONSHIP_NOT_REMOVED_FOR_DELETED_NODE( "node was deleted but relationship was not removed" ),
        PROPERTY_NOT_REMOVED_FOR_DELETED_NODE( "node was deleted but property was not removed" ),
        PROPERTY_NOT_REMOVED_FOR_DELETED_RELATIONSHIP( "relationship was deleted but property was not removed" ),
        REMOVED_RELATIONSHIP_STILL_REFERENCED( "removed relationship record still referenced" ),
        REMOVED_PROPERTY_STILL_REFERENCED( "removed property record still referenced" ),
        NEXT_DYNAMIC_NOT_REMOVED( "dynamic record removed, but next referenced in chain still in use" );
        private final String message;
        private final boolean warning;

        private ReferenceInconsistency( String message )
        {
            this.warning = false;
            this.message = message;
        }

        private ReferenceInconsistency( boolean warning, String message )
        {
            this.warning = warning;
            this.message = message;
        }

        @Override
        public String message()
        {
            return message;
        }

        @Override
        public boolean isWarning()
        {
            return warning;
        }
    }
    
    class PropertyOwnerInconsistency implements InconsistencyType
    {
        public enum OwnerInconsistencyType
        {
            MULTIPLE_OWNERS( "multiple owners for " ),
            PROPERTY_CHANGED_FOR_WRONG_OWNER( "property changed claimed an owner that did not contain the property record in its chain:\n\t" );
            private final String message;

            private OwnerInconsistencyType( String message )
            {
                this.message = message;
            }

            public InconsistencyType forProperties( Collection<PropertyRecord> properties )
            {
                return new PropertyOwnerInconsistency( this, properties.toArray( new PropertyRecord[properties.size()] ) );
            }

            public InconsistencyType forProperty( PropertyRecord property )
            {
                return new PropertyOwnerInconsistency( this, property );
            }
        }
        private final OwnerInconsistencyType type;
        private final PropertyRecord[] properties;
        
        private PropertyOwnerInconsistency( OwnerInconsistencyType type, PropertyRecord... properties )
        {
            this.type = type;
            this.properties = properties;
        }
        
        @Override
        public String message()
        {
            StringBuilder message = new StringBuilder( type.message );
            for ( int i = 0; i < properties.length; i++ )
            {
                if ( i > 0 ) message.append( "\n\t" );
                message.append( properties[i] );
            }
            return message.toString();
        }

        @Override
        public boolean isWarning()
        {
            return false;
        }
    }

    class PropertyBlockInconsistency implements InconsistencyType
    {
        public enum BlockInconsistencyType
        {
            INVALID_PROPERTY_KEY( "invalid key id of " ),
            UNUSED_PROPERTY_KEY( "key not in use for " ),
            ILLEGAL_PROPERTY_TYPE( "illegal property type for " ),
            DYNAMIC_NOT_IN_USE( "first dynamic record not in use for " );
            private final String message;

            private BlockInconsistencyType( String message )
            {
                this.message = message;
            }

            public InconsistencyType forBlock( PropertyBlock block )
            {
                return new PropertyBlockInconsistency( this, block );
            }
        }

        private final BlockInconsistencyType type;
        private final PropertyBlock block;

        private PropertyBlockInconsistency( BlockInconsistencyType type, PropertyBlock block )
        {
            this.type = type;
            this.block = block;
        }

        @Override
        public String message()
        {
            return type.message + block;
        }

        @Override
        public boolean isWarning()
        {
            return false;
        }
    }
}
