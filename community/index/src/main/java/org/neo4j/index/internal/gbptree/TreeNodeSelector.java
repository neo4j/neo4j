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
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;

/**
 * Able to select implementation of {@link TreeNode} to use in different scenarios, should be used in favor of directly
 * instantiating {@link TreeNode} instances.
 */
class TreeNodeSelector
{
    /**
     * Creates {@link TreeNodeFixedSize} instances.
     */
    static Factory FIXED = new Factory()
    {
        @Override
        public <KEY,VALUE> TreeNode<KEY,VALUE> create( int pageSize, Layout<KEY,VALUE> layout )
        {
            return new TreeNodeFixedSize<>( pageSize, layout );
        }

        @Override
        public byte formatIdentifier()
        {
            return TreeNodeFixedSize.FORMAT_IDENTIFIER;
        }

        @Override
        public byte formatVersion()
        {
            return TreeNodeFixedSize.FORMAT_VERSION;
        }
    };

    /**
     * Creates {@link TreeNodeDynamicSize} instances.
     */
    static Factory DYNAMIC = new Factory()
    {
        @Override
        public <KEY,VALUE> TreeNode<KEY,VALUE> create( int pageSize, Layout<KEY,VALUE> layout )
        {
            return new TreeNodeDynamicSize<>( pageSize, layout );
        }

        @Override
        public byte formatIdentifier()
        {
            return TreeNodeDynamicSize.FORMAT_IDENTIFIER;
        }

        @Override
        public byte formatVersion()
        {
            return TreeNodeDynamicSize.FORMAT_VERSION;
        }
    };

    /**
     * Selects a format based on the given {@link Layout}.
     *
     * @param layout {@link Layout} dictating which {@link TreeNode} to instantiate.
     * @return a {@link Factory} capable of instantiating the selected format.
     */
    static Factory selectByLayout( Layout<?,?> layout )
    {
        // For now the selection is done in a simple fashion, by looking at layout.fixedSize().
        return layout.fixedSize() ? FIXED : DYNAMIC;
    }

    /**
     * Selects a format based on the given format specification.
     *
     * @param formatIdentifier format identifier, see {@link Meta#getFormatIdentifier()}
     * @param formatVersion format version, see {@link Meta#getFormatVersion()}.
     * @return a {@link Factory} capable of instantiating the selected format.
     */
    static Factory selectByFormat( byte formatIdentifier, byte formatVersion )
    {
        // For now do a simple selection of the two formats we know. Moving forward this can contain
        // many more identifiers and different versions of each.
        if ( formatIdentifier == TreeNodeFixedSize.FORMAT_IDENTIFIER && formatVersion == TreeNodeFixedSize.FORMAT_VERSION )
        {
            return FIXED;
        }
        else if ( formatIdentifier == TreeNodeDynamicSize.FORMAT_IDENTIFIER && formatVersion == TreeNodeDynamicSize.FORMAT_VERSION )
        {
            return DYNAMIC;
        }
        throw new IllegalArgumentException(
                format( "Unknown format identifier:%d and version:%d combination", formatIdentifier, formatVersion ) );
    }

    /**
     * Able to instantiate {@link TreeNode} of a specific format and version.
     */
    interface Factory
    {
        /**
         * Instantiates a {@link TreeNode} of a specific format and version that this factory represents.
         *
         * @param pageSize page size, i.e. size of tree nodes.
         * @param layout {@link Layout} that will be used in this format.
         * @return the instantiated {@link TreeNode}.
         */
        <KEY,VALUE> TreeNode<KEY,VALUE> create( int pageSize, Layout<KEY,VALUE> layout );

        /**
         * Specifies the format identifier of the physical layout of tree nodes.
         * A format identifier must be unique among all possible existing format identifiers.
         * It's used to differentiate between different types of formats.
         * On top of this a specific {@link #formatVersion() format version} can specify a version of this format.
         *
         * @return format identifier for the specific {@link TreeNode} that this factory represents.
         * Can return this w/o instantiating the {@link TreeNode}.
         */
        byte formatIdentifier();

        /**
         * Specifies the version of this particular {@link #formatIdentifier() format}. It must be unique
         * among all other versions of this {@link #formatIdentifier() format}.
         *
         * @return format version for the specific {@link TreeNode} that this factory represents.
         * Can return this w/o instantiating the {@link TreeNode}.
         */
        byte formatVersion();
    }
}
