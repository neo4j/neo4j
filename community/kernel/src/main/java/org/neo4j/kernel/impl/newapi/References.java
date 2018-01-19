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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

/**
 * Utility class for managing reference encoding.
 *
 * The reason we need to encode references is that there are dense and non-dense nodes. A dense node will have a
 * pointer into the relationship group store, while a non-dense node points directly to the relationship store. On
 * retrieving a relationship reference from a dense node, we therefore have to transparently encode in the reference
 * that it actually points to a group. When the kernel then serves a relationship cursor using the reference, we need
 * to silently detect that we have a group reference, parse the groups, and setup the cursor to serve relationship
 * via this mode instead.
 *
 * The opposite problem also appears when the user acquires a relationship group reference from a non-dense node. See
 * {@link org.neo4j.kernel.impl.newapi.Read#relationships(long, long,
 * org.neo4j.internal.kernel.api.RelationshipTraversalCursor)} for more details.
 *
 * Node that {@code -1} is used to encode {@link AbstractBaseRecord#NO_ID that a reference is invalid}. In terms of
 * encoding {@code -1} is considered to have all flags, to setting one will not change {@code -1}. This however also
 * means that calling code must check for {@code -1} references before checking flags existence.
 *
 * Finally, an encoded reference cannot be used directly as an id to acquire the referenced object. Before using
 * the reference, the encoding must be cleared with {@link References#clearEncoding(long)}. To guard against using an
 * encoded reference, all encoded references are marked so they appear negative.
 */
class References
{
    private static final long FLAG_MARKER = 0x8000_0000_0000_0000L;
    private static final long FLAG_MASK = 0x7000_0000_0000_0000L;
    private static final long FLAGS = 0xF000_0000_0000_0000L;

    /**
     * Clear all encoding from a reference.
     *
     * @param reference The reference to clear.
     * @return The cleared reference.
     */
    static long clearEncoding( long reference )
    {
        assert reference != NO_ID;
        return reference & ~FLAGS;
    }

    static class Relationship
    {
        /** @see #encodeForFiltering(long) */
        private static final long FILTER                = 0x1000_0000_0000_0000L;

        /** @see #encodeForTxStateFiltering(long) */
        private static final long FILTER_TX_STATE       = 0x2000_0000_0000_0000L;

        /** @see #encodeFromGroup(long) */
        private static final long GROUP                 = 0x3000_0000_0000_0000L;

        /** @see #encodeNoOutgoingRels(int) */
        private static final long NO_OUTGOING_OF_TYPE   = 0x4000_0000_0000_0000L;

        /** @see #encodeNoIncomingRels(int) */
        private static final long NO_INCOMING_OF_TYPE   = 0x5000_0000_0000_0000L;

        /** @see #encodeNoLoopRels(int) */
        private static final long NO_LOOP_OF_TYPE       = 0x6000_0000_0000_0000L;

        /**
         * Encode a group id as a relationship reference.
         */
        static long encodeFromGroup( long groupId )
        {
            return groupId | GROUP | FLAG_MARKER;
        }

        /**
         * Check whether a relationship reference is an encoded group id.
         */
        static boolean isGroup( long relationshipReference )
        {
            assert relationshipReference != NO_ID;
            return (relationshipReference & FLAG_MASK) == GROUP;
        }

        /**
         * Encode that the relationship id needs filtering by it's first element.
         */
        static long encodeForFiltering( long relationshipId )
        {
            return relationshipId | FILTER | FLAG_MARKER;
        }

        /**
         * Check whether a relationship reference is a relationship id marked for filtering.
         */
        static boolean isFilter( long relationshipReference )
        {
            assert relationshipReference != NO_ID;
            return (relationshipReference & FLAG_MASK) == FILTER;
        }

        /**
         * Encode that the relationship id needs filtering by it's first element.
         */
        static long encodeForTxStateFiltering( long relationshipId )
        {
            return relationshipId | FILTER_TX_STATE | FLAG_MARKER;
        }

        /**
         * Check whether a relationship reference is a relationship id marked for tx-state filtering.
         */
        static boolean isTxStateFilter( long relationshipReference )
        {
            assert relationshipReference != NO_ID;
            return (relationshipReference & FLAG_MASK) == FILTER_TX_STATE;
        }

        /**
         * Encode that no outgoing relationships of the encoded type exist.
         */
        static long encodeNoOutgoingRels( int type )
        {
            return type | NO_OUTGOING_OF_TYPE | FLAG_MARKER;
        }

        /**
         * Check whether this reference encode that no outgoing relationships of the encoded type exist.
         */
        static boolean isNoOutgoing( long relationshipReference )
        {
            assert relationshipReference != NO_ID;
            return (relationshipReference & FLAG_MASK) == NO_OUTGOING_OF_TYPE;
        }

        /**
         * Encode that no incoming relationships of the encoded type exist.
         */
        static long encodeNoIncomingRels( int type )
        {
            return type | NO_INCOMING_OF_TYPE | FLAG_MARKER;
        }

        /**
         * Check whether this reference encode that no incoming relationships of the encoded type exist.
         */
        static boolean isNoIncoming( long relationshipReference )
        {
            assert relationshipReference != NO_ID;
            return (relationshipReference & FLAG_MASK) == NO_INCOMING_OF_TYPE;
        }

        /**
         * Encode that no loop relationships of the encoded type exist.
         */
        static long encodeNoLoopRels( int type )
        {
            return type | NO_LOOP_OF_TYPE | FLAG_MARKER;
        }

        /**
         * Check whether this reference encode that no loop relationships of the encoded type exist.
         */
        static boolean isNoLoop( long relationshipReference )
        {
            assert relationshipReference != NO_ID;
            return (relationshipReference & FLAG_MASK) == NO_LOOP_OF_TYPE;
        }
    }

    static class Group
    {
        private static final long DIRECT = 0x1000_0000_0000_0000L;

        /**
         * Encode a relationship id as a group reference.
         */
        static long encodeRelationship( long relationshipId )
        {
            return relationshipId | DIRECT | FLAG_MARKER;
        }

        /**
         * Check whether a group reference is an encoded relationship id.
         */
        static boolean isRelationship( long groupReference )
        {
            assert groupReference != NO_ID;
            return (groupReference & FLAG_MASK) == DIRECT;
        }
    }
}
