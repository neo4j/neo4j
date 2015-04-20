/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.function.primitive.FunctionFromPrimitiveLongLongToPrimitiveLong;
import org.neo4j.function.primitive.PrimitiveLongPredicate;
import org.neo4j.helpers.CloneableInPublic;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

/**
 * Keeps track of the current load position(s) for loading relationships for a particular node.
 * A {@link NodeImpl cached node} caches relationships as it loads them. Loading occurs in batches
 * and so a node may not have loaded all its relationship at any given point in time. Since loading happens
 * in batches, a batch will continue to load from where the previous batch ended. This state is exactly
 * what instances of this type keeps track of. All its methods accepts direction and type since dense nodes
 * have individual relationship chains per direction and type.
 */
public interface RelationshipLoadingPosition extends CloneableInPublic
{
    /**
     * Returns the relationship id that a chain for the given direction and type(s) is at.
     *
     * @param direction relationship direction. If {@link DirectionWrapper#BOTH} is supplied then
     * all directions are tried and first non-ended chain will be returned, otherwise the supplied direction
     * and loops are considered.
     * @param types a list of types to try. The first encountered chain that is not at its end will be used.
     * @return position, i.e. relationship id to start loading from for the first encountered non-ended direction
     * and first encountered non-ended type.
     */
    long position( DirectionWrapper direction, int[] types );

    /**
     * Goes to and returns the next relationship id in a chain for the given direction and type(s).
     *
     * @param relationship id to have this relationship chain position point to. In a multi-chain
     * scenario a value of {@link Record#NO_NEXT_RELATIONSHIP} will trigger moving over to the next
     * relationship chain matching the given {@code direction} and {@code types}.
     * @param direction relationship direction. If {@link DirectionWrapper#BOTH} is supplied then
     * all directions are tried and first non-ended chain will be returned, otherwise the supplied direction
     * and loops are considered.
     * @param types a list of types to try. The first encountered chain that is not at its end will be used.
     * @return next position in the chain, i.e. next relationship id to load for the first encountered
     * non-ended direction and first encountered non-ended type. For a single chain the returned value
     * will always be the specified {@code position}, but in a multi-chain scenario the end of one chain
     * will instead return the position of another chain.
     */
    long nextPosition( long position, DirectionWrapper direction, int[] types );

    /**
     * Checks whether or not there are any more relationships in a chain for the given direction and type(s).
     *
     * @param direction relationship direction. If {@link DirectionWrapper#BOTH} is supplied then
     * all directions are tried and first non-ended chain will be returned, otherwise the supplied direction
     * and loops are considered.
     * @param types a list of types to try. The first encountered chain that is not at its end will be used.
     * @return {@code true} if a non-ended chain for the given direction and type(s) was found.
     */
    boolean hasMore( DirectionWrapper direction, int[] types );

    /**
     * Checks whether or not the chain for the given direction and type is currently at the given position.
     *
     * @param direction relationship direction. A value of {@link DirectionWrapper#BOTH} means the loop chain.
     * @param type relationship type.
     * @return {@code true} if the chain for the given direction and type is currently at the given position,
     * otherwise {@code false}.
     */
    boolean atPosition( DirectionWrapper direction, int type, long position );

    /**
     * Used when relationships gets deleted in the middle of traversing their chain(s). Should only be called if
     * {@link #atPosition(PrimitiveLongPredicate)} returns {@code true}. Current positions can here be
     * moved to the next in use relationship if the current position happens to point to a deleted relationship.
     * If a current position isn't at a deleted relationship then the {@code next} function returns whatever
     * was passed in.
     *
     * @param nodeId node id of this chain position. Used for passing back into {@code next}.
     * @param next function for getting the next in use relationship after a currently deleted position.
     */
    void patchPosition( long nodeId, FunctionFromPrimitiveLongLongToPrimitiveLong<RuntimeException> next );

    @Override
    RelationshipLoadingPosition clone();

    public static final RelationshipLoadingPosition EMPTY = new RelationshipLoadingPosition()
    {
        @Override
        public long position( DirectionWrapper direction, int[] types )
        {
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        @Override
        public long nextPosition( long position, DirectionWrapper direction, int[] types )
        {
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }

        @Override
        public boolean hasMore( DirectionWrapper direction, int[] types )
        {
            return false;
        }

        @Override
        public boolean atPosition( DirectionWrapper direction, int type, long position )
        {
            return false;
        }

        @Override
        public void patchPosition( long nodeId, FunctionFromPrimitiveLongLongToPrimitiveLong<RuntimeException> next )
        {
        }

        @Override
        public RelationshipLoadingPosition clone()
        {
            return this;
        }
    };
}
