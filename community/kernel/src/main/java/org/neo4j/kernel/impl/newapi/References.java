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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

/**
 * Utility class for managing flags or references.
 *
 * The reason we need flags on references is that there are dense and non-dense nodes. A dense node will have a
 * reference into the relationship group store, while a non-dense node points directly to the relationship store. On
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
 * flags {@code -1} is considered to have all flags, to setting one will not change {@code -1}. This however also
 * means that calling code must check for {@code -1} references before checking flags existence.
 *
 * Finally, a flagged reference cannot be used directly as an offset to acquire the referenced object. Before using
 * the reference, flags must be cleared with {@link References#clearFlags(long)}. To guard against using a flagged
 * reference, flagged references are marked to they appear negative.
 */
class References
{
    private static final long FLAG_MARKER = 0x8000_0000_0000_0000L;
    private static final long FLAGS = 0xF000_0000_0000_0000L;

    // Relationship references
    private static final long FILTER_FLAG = 0x2000_0000_0000_0000L;
    private static final long GROUP_FLAG = 0x4000_0000_0000_0000L;

    // Relationship group references
    private static final long DIRECT_FLAG = 0x2000_0000_0000_0000L;

    // Use node reference as property reference
    private static final long STATE_MARKER = 0x1000_0000_0000_0000L;
    private static final long NODE_MARKER = 0x2000_0000_0000_0000L;
    private static final long RELATIONSHIP_MARKER = 0x4000_0000_0000_0000L;

    /**
     * Clear all flags from a reference.
     * @param reference The reference to clear.
     * @return The cleared reference.
     */
    static long clearFlags( long reference )
    {
        assert reference != NO_ID;
        return reference & ~FLAGS;
    }

    // Relationship references

    /**
     * Add group flag to relationship reference.
     * @param relationshipReference The reference to flag.
     * @return The flagged reference.
     */
    static long setGroupFlag( long relationshipReference )
    {
        return relationshipReference | GROUP_FLAG | FLAG_MARKER;
    }

    /**
     * Check whether a relationship reference has the group flag.
     * @param relationshipReference the reference to check
     * @return true if the flag is set
     */
    static boolean hasGroupFlag( long relationshipReference )
    {
        assert relationshipReference != NO_ID;
        return (relationshipReference & GROUP_FLAG) != 0L;
    }

    /**
     * Add filter flag to relationship reference.
     * @param relationshipReference The reference to flag.
     * @return The flagged reference.
     */
    static long setFilterFlag( long relationshipReference )
    {
        return relationshipReference | FILTER_FLAG | FLAG_MARKER;
    }

    /**
     * Check whether a relationship reference has the filter flag.
     * @param relationshipReference the reference to check
     * @return true if the flag is set
     */
    static boolean hasFilterFlag( long relationshipReference )
    {
        assert relationshipReference != NO_ID;
        return (relationshipReference & FILTER_FLAG) != 0L;
    }

    // Relationship group references

    /**
     * Add direct flag to relationship group reference.
     * @param groupReference The reference to flag.
     * @return The flagged reference.
     */
    static long setDirectFlag( long groupReference )
    {
        return groupReference | DIRECT_FLAG | FLAG_MARKER;
    }

    /**
     * Check whether a relationship reference has the direct flag.
     *
     * @param groupReference the reference to check
     * @return true if the flag is set
     */
    static boolean hasDirectFlag( long groupReference )
    {
        assert groupReference != NO_ID;
        return (groupReference & DIRECT_FLAG) != 0L;
    }

    /**
     * Encode a node id as a property reference.
     *
     * This allows us to encode a node reference as a property reference so that we can do lookups of changes in the
     * transaction state.
     * @param nodeReference the reference to encode
     * @return The encoded reference
     */
    static long setNodeFlag( long nodeReference )
    {
        return nodeReference | NODE_MARKER | FLAG_MARKER;
    }

    /**
     * Checks if the property reference really is a node reference
     *
     * @param reference The reference to check
     * @return <tt>true</tt>if the reference refers to a node otherwise <tt>false</tt>
     */
    static boolean hasNodeFlag( long reference )
    {
        assert reference != NO_ID;
        return (reference & NODE_MARKER) != 0L;
    }

    /**
     * Encode a relationship id as a property reference.
     *
     * This allows us to encode a relationship reference as a property reference so that we can do lookups of changes in the
     * transaction state.
     * @param relationshipReference the reference to encode
     * @return The encoded reference
     */
    static long setRelationshipFlag( long relationshipReference )
    {
        return relationshipReference | RELATIONSHIP_MARKER | FLAG_MARKER;
    }

    /**
     * Checks if the property reference really is a relationship reference
     *
     * @param reference The reference to check
     * @return <tt>true</tt>if the reference refers to a relationship otherwise <tt>false</tt>
     */
    static boolean hasRelationshipFlag( long reference )
    {
        assert reference != NO_ID;
        return (reference & RELATIONSHIP_MARKER) != 0L;
    }

    /**
     * Marks the property reference as having changes in the transaction state.
     * <p>
     * Setting this flag tells us that the value on disk is not the only source of truth, the transaction state must
     * also be checked.
     *
     * @param reference The reference to set as having tx state
     * @return The encoded reference.
     */
    static long setTxStateFlag( long reference )
    {
        return reference | STATE_MARKER | FLAG_MARKER;
    }

    /**
     * Checks if the property reference has been marked as having changes in the transaction state.
     * @param reference The reference to check
     * @return <tt>true</tt>if the reference has been marked as having changes otherwise <tt>false</tt>
     */
    static boolean hasTxStateFlag( long reference )
    {
        assert reference != NO_ID;
        return (reference & STATE_MARKER) != 0L;
    }
}
