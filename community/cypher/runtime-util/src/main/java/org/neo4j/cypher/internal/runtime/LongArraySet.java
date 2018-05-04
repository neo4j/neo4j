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
package org.neo4j.cypher.internal.runtime;

/**
 * When you need to have a set of arrays of longs representing entities - look no further
 * <p>
 * This set will keep all it's state in a single long[] array, marking unused slots
 * using 0xF000000000000000L, a value that should never be used for node or relationship id's.
 * <p>
 * The set will be resized when either probing has to go on for too long when doing inserts,
 * or the load factor reaches 75%.
 *
 * The word "offset" here means the index into an array,
 * and slot is a number that multiplied by the width of the values will return the offset.
 */
public class LongArraySet
{
    private static final long NOT_IN_USE = -2;

    private static final int SLOT_EMPTY = 0;
    private static final int VALUE_FOUND = 1;
    private static final int CONTINUE_PROBING = -1;

    private Table table;
    private final int width;

    public LongArraySet( int initialCapacity, int width )
    {
        assert (initialCapacity & (initialCapacity - 1)) == 0 : "Size must be a power of 2";
        assert width > 0 : "Number of elements must be larger than 0";

        this.width = width;
        table = new Table( initialCapacity );
    }

    /**
     * Adds a value to the set.
     *
     * @param value The new value to be added to the set
     * @return The method returns true if the value was added and false if it already existed in the set.
     */
    public boolean add( long[] value )
    {
        assert value.length == width : "all elements must have the same size";
        int slotNr = slotFor( value );
        while ( true )
        {
            int offset = slotNr * width;
            if ( table.inner[offset] == NOT_IN_USE )
            {
                if ( table.timeToResize() )
                {
                    // We know we need to add the value to the set, but there is no space left
                    resize();
                    // Need to restart linear probe after resizing
                    slotNr = slotFor( value );
                }
                else
                {
                    table.setValue( slotNr, value );
                    return true;
                }
            }
            else
            {
                for ( int i = 0; i < width; i++ )
                {
                    if ( table.inner[offset + i] != value[i] )
                    {
                        // Found a different value in this slot
                        slotNr = (slotNr + 1) & table.tableMask;
                        break;
                    }
                    else if ( i == width - 1 )
                    {
                        return false;
                    }
                }
            }
        }
    }

    /***
     * Returns true if the value is in the set.
     * @param value The value to check for
    * @return whether the value is in the set or not.
     */
    public boolean contains( long[] value )
    {
        assert value.length == width : "all elements must have the same size";
        int slot = slotFor( value );

        int result;
        do
        {
            result = table.checkSlot( slot, value );
            slot = (slot + 1) & table.tableMask;
        }
        while ( result == CONTINUE_PROBING );
        return result == VALUE_FOUND;
    }

    private int hashCode( long[] arr, int from, int numberOfElements )
    {
        int h = 1;
        for ( int i = from; i < from + numberOfElements; i++ )
        {
            long element = arr[i];
            int elementHash = (int) (element ^ (element >>> 32));
            h = 31 * h + elementHash;
        }

        // return h; // Uncomment this to go back to normal hashing

        // This is tabulation hashing!
        int a1 = tab[0][h & 0xff];
        int a2 = tab[1][(h >>> 8) & 0xff];
        int a3 = tab[2][(h >>> 16) & 0xff];
        int a4 = tab[3][(h >>> 24) & 0xff];
        return a1 ^ a2 ^ a3 ^ a4;
    }

    private void resize()
    {
        int oldSize = table.capacity;
        int oldNumberEntries = table.numberOfEntries;
        long[] srcArray = table.inner;
        table = new Table( oldSize * 2 );
        long[] dstArray = table.inner;
        table.numberOfEntries = oldNumberEntries;

        for ( int fromOffset = 0; fromOffset < oldSize * width; fromOffset = fromOffset + width )
        {
            if ( srcArray[fromOffset] != NOT_IN_USE )
            {
                int toSlot = hashCode( srcArray, fromOffset, width ) & table.tableMask;

                if ( dstArray[toSlot * width] != NOT_IN_USE )
                {
                    // Linear probe until we find an unused slot.
                    // No need to check for size here - we are already inside of resize()
                    toSlot = findUnusedSlot( dstArray, toSlot );
                }
                System.arraycopy( srcArray, fromOffset, dstArray, toSlot * width, width );
            }
        }
    }

    private int findUnusedSlot( long[] to, int fromSlot )
    {
        while ( true )
        {
            if ( to[fromSlot * width] == NOT_IN_USE )
            {
                return fromSlot;
            }
            fromSlot = (fromSlot + 1) & table.tableMask;
        }
    }

    private int slotFor( long[] value )
    {
        return hashCode( value, 0, width ) & table.tableMask;
    }

    class Table
    {
        private final int capacity;
        private final long[] inner;
        int numberOfEntries;
        private int resizeLimit;

        int tableMask;

        Table( int capacity )
        {
            this.capacity = capacity;
            resizeLimit = (int) (capacity * 0.75);
            tableMask = Integer.highestOneBit( capacity ) - 1;
            inner = new long[capacity * width];
            java.util.Arrays.fill( inner, NOT_IN_USE );
        }

        boolean timeToResize()
        {
            return numberOfEntries == resizeLimit;
        }

        int checkSlot( int slot, long[] value )
        {
            assert value.length == width;

            int startOffset = slot * width;
            if ( inner[startOffset] == NOT_IN_USE )
            {
                return SLOT_EMPTY;
            }

            for ( int i = 0; i < width; i++ )
            {
                if ( inner[startOffset + i] != value[i] )
                {
                    return CONTINUE_PROBING;
                }
            }

            return VALUE_FOUND;
        }

        void setValue( int slot, long[] value )
        {
            int offset = slot * width;
            System.arraycopy( value, 0, inner, offset, width );
            numberOfEntries++;
        }
    }

    // Semi random numbers to use for tabulation hashing
    private static int[][] tab =
            {{0x0069aeff, 0x6ac0719e, 0x384cd7ee, 0xcba78313, 0x133ef89a, 0xb37979e6, 0xa4c4e09c, 0x911c738b, 0xc7fe9194, 0xba8e5dc7, 0xe610718c, 0x48460ac5,
                    0x6b4d9d43, 0x73afeeab, 0x051264cb, 0x4b3dba93, 0x28837665, 0xfb80a52b, 0xad1c14af, 0xb2baf17f, 0x35e311a5, 0xf7fa2905, 0xa973c315,
                    0x00885f47, 0x8842622b, 0x0445a92c, 0x701ba3a0, 0xef608902, 0x176099ad, 0xd240f938, 0xb32d83c6, 0xb341afb8, 0xc3a978fb, 0x55ed1f0c,
                    0xb581286e, 0x8ff6938e, 0x9f11c1c5, 0x4d083bd6, 0x1aacc2a4, 0xdf13f00a, 0x1e282712, 0x772d354b, 0x21e3a7fd, 0x4bc932dc, 0xe1deb7ba,
                    0x5e868b8a, 0xc9331cc6, 0xaa931bbf, 0xff92aba6, 0xe3efc69f, 0xda3b8e2a, 0xf9b21ec1, 0x2fb89674, 0x61c87462, 0xa553c2f9, 0xca01e279,
                    0x35999337, 0xf44c4fd3, 0x136a2773, 0x812607a8, 0xbfcd9bbf, 0x0b1d15cd, 0xc2a0038b, 0x029ab4f7, 0xcd7c58f9, 0xed3821c4, 0x325457c6,
                    0x1dc6b295, 0x876dcb83, 0x52df45fc, 0xa01c9fba, 0xc938ff66, 0x19e52c87, 0x03ae67f9, 0x7db39e51, 0x74f31686, 0x5f10e5a3, 0x74108d8a,
                    0x64e63104, 0xd86a38d6, 0x65be2fbb, 0xef06049e, 0x9bca1dbd, 0x06c63e73, 0xe97bd103, 0xfed3c22c, 0x09d10fc6, 0xb92633a3, 0x21378ebf,
                    0xe37fa54e, 0x893c7910, 0xc1c74a5a, 0x6c23c029, 0x4d4b6187, 0xd72bb8fb, 0x0dbe1118, 0x5e0f4188, 0xce0d2dc8, 0x8dd83231, 0x0466ab90,
                    0x814bc11a, 0xef688b9b, 0x0a03c851, 0xca3c984f, 0x6df87ca4, 0x6b34d1b2, 0x2bad5c75, 0xaed1b6d8, 0x8c73f8b4, 0x4577d798, 0x5c953767,
                    0xe7da2d51, 0x2b9279a0, 0x418d9b51, 0x8c47ec3d, 0x894e6119, 0xa0ca769d, 0x1c3b16a4, 0xa1621b5b, 0xa695da53, 0x22462819, 0xf4b878cf,
                    0x72b4d648, 0x1faf4267, 0x4ba16750, 0x08a9d645, 0x6bfb829c, 0xe051295f, 0x6dd5cd97, 0x2e9d1baf, 0x6ed6231d, 0x6f84cb25, 0x9ae60c95,
                    0xbcee55ca, 0x6831cd97, 0x2ccdbc99, 0x9f8a0a81, 0xa0b2c08f, 0xe957c36b, 0x9cb797b5, 0x107c6362, 0x48dacf5d, 0x6e16f569, 0x39be78c3,
                    0x6445637f, 0xed445ee5, 0x8ec45004, 0x9ef8a405, 0xb5796a45, 0x049d5143, 0xb3c1d852, 0xc36d9b44, 0xab0da981, 0xff5226b3, 0x19169b4c,
                    0x9a49194d, 0xba218b42, 0xab98c8ee, 0x4db02645, 0x6faca3c8, 0x12c60d2d, 0xaf67b750, 0xf0f6a855, 0xead566d9, 0x42d0cccd, 0x76a532bb,
                    0x82a6dc35, 0xc1c23d0e, 0x83d45bd2, 0xd7024912, 0x97888901, 0x2b7cdd2c, 0x523742a5, 0xecb96b3b, 0xd800d833, 0x7b4d0c91, 0x95c7dd86,
                    0x88880aad, 0xf0ce0990, 0x7e292a90, 0x79ac4437, 0x8a9f59cc, 0x818444d1, 0xae4e735d, 0xa529db95, 0x58b35661, 0xa909a7de, 0x9273beaa,
                    0xfe94332c, 0x259b88e4, 0xc88f4f6a, 0x2a9d33ef, 0x4b5d106d, 0xdc3a9fca, 0xa8061cad, 0x7679422c, 0xaf72ad02, 0xc5799ea5, 0x306d694d,
                    0x620aad10, 0xd188b9dd, 0xeff6ad87, 0x6b890354, 0xb5907cd3, 0x733290fc, 0x4b6c0733, 0x0bad0ebd, 0xa049d3ad, 0xc9d0cdae, 0x9c144d6f,
                    0x5990b63b, 0xfa33d8e2, 0x9ebeb5a0, 0xbc7c5c92, 0xd3edd2e6, 0x54ae1af6, 0xd6ada4bd, 0x14094c5a, 0x0e3c5adf, 0xf1ab60f1, 0x74456a66,
                    0x0f3a675a, 0x87445d0d, 0xa81adc2e, 0x0f47a1a5, 0x4eedb844, 0x9c9cb0ce, 0x8bb3d330, 0x02df93e6, 0x86e3ad51, 0x1c1072b9, 0xacf3001b,
                    0xbd08c487, 0xc2667a11, 0xdd5ef664, 0xd47b67fb, 0x959cca45, 0xa7da8e68, 0xb75b1e18, 0x75201924, 0xe689ab8b, 0x0f5e6b0a, 0x75205923,
                    0xbba35593, 0xd24dab24, 0x0288caeb, 0xcbf022a9, 0x392d7ee5, 0x16fe493a, 0xb6bcadfd, 0x9813ec72, 0x9aa3d37c, 0xee88a59e, 0x6cdbad4e,
                    0x6b96aabf, 0xcb54d5e5},
                    {0x116fc403, 0x260d7e7b, 0xdef689e7, 0xa5b3d49a, 0x921f3594, 0xb24c8cba, 0x1bdefb3f, 0x6519e846, 0x24b37253, 0x1cc6b12b, 0x6f48f06e,
                            0xca90b0db, 0x8e20570b, 0xda75ed0f, 0x1b515143, 0x0990a659, 0xdcedb6b3, 0xec22de79, 0xdd56f7a9, 0x901194a6, 0x4bf3db02, 0x5d31787d,
                            0xd24da2ca, 0x9fc9bc14, 0x9aa38ac9, 0xe95972ba, 0x8233a732, 0xb9d4317e, 0x51f9b329, 0x94f12c56, 0x1ace26e4, 0xecda5183, 0x1353e547,
                            0x39b99ab3, 0x6413ac97, 0xeb6b5334, 0xdd94ed2b, 0x298e9d2c, 0xd38abc91, 0x3f17ee4e, 0x99f8931d, 0x88bae7da, 0xb5506a36, 0x2d7baf6d,
                            0x42a98d2b, 0xbb9b94b9, 0x58820083, 0x521bba4c, 0x76699597, 0x137b86be, 0x8533888e, 0xb37316dd, 0x284c3de4, 0xfe60e3e6, 0x94edaa40,
                            0x919c85cd, 0x24cb6f23, 0x6b446fbd, 0xbe933c15, 0x2a43951a, 0x791a9f90, 0x47977c04, 0xa6350eec, 0x95e817a5, 0xffc82e8c, 0xad379229,
                            0x6ec9531a, 0x8cab29f9, 0xb2f18402, 0xd0ebdac1, 0xd7b559b4, 0x7ad30e7c, 0xe1d1adb7, 0x58a66f9c, 0x7a26636a, 0x8c865f92, 0x65363517,
                            0x732b87db, 0x64a1ad52, 0x72e87c39, 0x0b943e4d, 0x532d3593, 0xedcf9975, 0x44b5bec1, 0x13ac91f8, 0x6e6f3a76, 0x36ac3c6d, 0x528a3ecf,
                            0xf3d8cd75, 0x8facd64c, 0xdb4d13d5, 0x80d49a67, 0xaa7061d3, 0x9486ba8d, 0x7454a65b, 0x18e7b707, 0xd9cc05b9, 0x44eb014d, 0x28ba26d8,
                            0xa8852791, 0xf8dc3053, 0xabe46b52, 0x9e261d1f, 0x768f83dd, 0x1c888838, 0x6d9b9ce6, 0x69e82575, 0x2959538f, 0xd0ff9685, 0x92b4540c,
                            0x7c93035b, 0x7cad90ad, 0x49aaa908, 0x3981f4b8, 0x191f4339, 0xd0971bfc, 0xa7209692, 0x0e253cad, 0x40e2ee61, 0xc5c63486, 0xdf4f238b,
                            0x2d3cb89a, 0x3b5704b2, 0xcc14c2cb, 0xb1698d38, 0x079c3b9b, 0xbb3867e4, 0x9f01e223, 0x35e69012, 0x5c87d888, 0x2cea4193, 0xee088da5,
                            0x0ea4d5ab, 0x8a4906e8, 0xf6e5e283, 0xee87fa18, 0x9f96c751, 0x947252c0, 0x9b50b97e, 0x05952521, 0x9440f5ae, 0xa0642786, 0xebcc62be,
                            0xadccf011, 0x00b863e6, 0x1c3ab5b3, 0x7c701e4b, 0xa9565792, 0xb1ad459c, 0x833ba164, 0x89544ae3, 0x35540c75, 0x198d0fec, 0xbe93bf33,
                            0xc28444b3, 0xbc3add48, 0xb4300c14, 0xee0ed408, 0xca08ada3, 0x0be06480, 0xc4dd8ce2, 0x61195564, 0x5b10a111, 0x65cd2b3b, 0xcbeb06ae,
                            0xfce70080, 0xef40b102, 0xfc0bfe6f, 0x8111bf20, 0xfb166db1, 0x3598b2ef, 0x1e0e04de, 0x1bf7cf2d, 0x0de7eaf1, 0x829457e0, 0xe8865341,
                            0x826272ad, 0xb57db2a4, 0x7413e6e7, 0x416323ff, 0x8e08d503, 0x1da4dfac, 0x983b9a78, 0x0fab5fe0, 0x585e7a90, 0x038cf73c, 0xecf90d31,
                            0x046055c8, 0x59926d71, 0x06959f1f, 0x3b8290b7, 0x0bb834d9, 0xa0dc5bec, 0xec9ae604, 0x6ebfd59d, 0xfeccbab5, 0x240bd4ba, 0x2df2b232,
                            0xe14e0383, 0xd86526ec, 0xe3d974fc, 0x940662b5, 0x81abf5d4, 0x8010e6eb, 0x700d9849, 0x040d0c42, 0xc980417b, 0x95fa374a, 0x724b1448,
                            0x217205ec, 0x0153b4bb, 0xea55ea92, 0x2049d5a1, 0x82576f06, 0x586fcfeb, 0xa975e489, 0x14c862e9, 0xacb8b52c, 0x2f3fb91e, 0xce273650,
                            0x66608f4a, 0x24f81bb7, 0x0382dc34, 0x07bdc163, 0xc42ad034, 0xe63cf998, 0x1a61f233, 0xd5754ebe, 0x37275214, 0x2322de2a, 0x3a53b9b4,
                            0xab9c6963, 0x2f3a51be, 0x5066e7c7, 0x941bda97, 0x75fadceb, 0xd05ad081, 0xf77d5daf, 0xd9879250, 0xebf8bf97, 0x65be4a70, 0x388eda48,
                            0x728173fb, 0x05975bfa, 0x314dad8a, 0x2cb4909f, 0xc736b716, 0x9007296d, 0x4fd61551, 0xd4378ccf, 0x649aac3e, 0xd9ca1a9d, 0x16ff16ae,
                            0x8090f1c5, 0xfe0c4703, 0xc4152307},
                    {0xf07e5e34, 0x62114ba6, 0xf45ffe22, 0xbaa48702, 0xe27e48a4, 0xc43b4779, 0x549a4566, 0x93bc4836, 0x3b2e8d46, 0x3f8a77ae, 0x71e2d944,
                            0xc09c5dce, 0xebfbfd4f, 0x7f8e1c40, 0x3c310a69, 0x52f62f09, 0xb7fd11bb, 0xa9d055a7, 0xe3bd4654, 0x9696ae10, 0xdf953225, 0x42fd2380,
                            0x69756e5c, 0x9d950bc4, 0xe2beea59, 0xd33daa07, 0xe97d31ce, 0xd9fb0a49, 0x553a27f2, 0x7166586f, 0xeb04d48c, 0x72adb63a, 0x340ab99e,
                            0x459b4609, 0x481421b7, 0x7db83c71, 0x192f6c22, 0x711852a8, 0xc6bd6562, 0xb91be2c8, 0xefe89dbf, 0xc404eb9b, 0x9ebc1bc7, 0x8dc7eed2,
                            0x4d84efd7, 0x0783d7e5, 0x3b5ca2f2, 0x9997e51c, 0x89b432c9, 0x72ae9672, 0x61d522d9, 0xa639fd45, 0xa7da3b46, 0x696e73ec, 0x89581a95,
                            0x4aa25f94, 0xd0eb2a48, 0x04865f68, 0x1cbd651a, 0xd6b2afd9, 0xd401b965, 0xd20aa5a7, 0xc0aa1b15, 0xfb4ce7af, 0x159974c5, 0x15d0841d,
                            0x6b2836b4, 0xef3b3edf, 0xaf2db0b3, 0x13106fb6, 0xff41d7f9, 0xab2a698d, 0x68e04dc9, 0xe5ee0099, 0xe50d4017, 0x5ea78d6d, 0x2e18fb07,
                            0xfe22b9ff, 0x544c05f1, 0xc2e10853, 0x8d151bd6, 0x17ee763a, 0xa663ce31, 0x4a4b5e33, 0x298b13c1, 0xd3b40c89, 0x121b6b4e, 0x59cf0429,
                            0x3d0bab9d, 0xd24c5dfe, 0x5bb7349f, 0xac5dbfe9, 0x7eca5ebb, 0xadb8b3e3, 0x71ab540b, 0xc8e3dc0d, 0x12e6cd3f, 0x8197f22c, 0x5ff77265,
                            0xe5641dbc, 0x818ab24c, 0x627b98f7, 0xdd84e1d6, 0x531c2346, 0xec2f4e3c, 0x4a3cb318, 0x70cb24fe, 0x35c17bfe, 0xec91fd18, 0x6efb3c18,
                            0x16908369, 0x41732188, 0x449e658b, 0x2e9931cb, 0x67cd066e, 0x883ca306, 0xf66aecac, 0x979bf015, 0x8e85e27d, 0x0560372b, 0x987995d6,
                            0xaff98ed7, 0x552ee87b, 0x21a53787, 0x3d3cfd45, 0xa084dae0, 0x8c91be2f, 0xac4c3550, 0xa7db63ff, 0x124b2f23, 0x95d05d4e, 0xb983db13,
                            0xa929a3c1, 0x111cd0a0, 0xf59ded9a, 0xce677ae3, 0xfa949e59, 0xd673e658, 0xf8c8e27b, 0x3c60fc3d, 0x59a4f230, 0xf54a5e87, 0x08cff440,
                            0xd4bbb1ee, 0x6a0c7db0, 0xecbaa99d, 0xec61dcaf, 0xf1056e2b, 0x54236899, 0xadad347c, 0xc9885bc9, 0x2fe2a4ec, 0x01ba2b86, 0x6b23f604,
                            0xb354ef08, 0x6a3dc5e2, 0xab61da36, 0x7543925a, 0x0a558940, 0x48d4d8f3, 0xd84f2f6f, 0x6ac5311c, 0xcd1b660e, 0x51293d3d, 0xa0f15790,
                            0xd629cd78, 0x89201fa5, 0x46005119, 0x9617fa14, 0xc375a68b, 0x7ccb519b, 0x6420a714, 0xb736d2ce, 0x154fcf4a, 0x71cad2f5, 0xacb150d7,
                            0x97bc8e36, 0xc5506d0a, 0xa9facc35, 0x1a9630db, 0xbd3d72ee, 0x58cdf27c, 0x17f3e1f9, 0x41598836, 0xd6adac30, 0x309a5b3f, 0x3bd3aa32,
                            0x40f08f50, 0xf37cbd6c, 0xcbdb8aef, 0xe0819189, 0x5a9b663b, 0x6932a448, 0xb1b3e866, 0xc50ee24d, 0xad999126, 0xafb04056, 0xc95974e5,
                            0x636a64fa, 0x0bb12dd9, 0x78caa164, 0xd26a7ec8, 0x451a0b53, 0x6d00aac6, 0x484d1d9d, 0x39728dd4, 0xfbfec2ea, 0xa6d5aaf9, 0x91c4f6ea,
                            0x31cab009, 0x9b6ba4e8, 0xe271ed67, 0x4c87a84d, 0x8a1a4567, 0x93749497, 0xc566edcc, 0xc8229554, 0x927925fd, 0xad1caced, 0xdc24f7ed,
                            0xc92b9220, 0x936cd037, 0xbd2d0256, 0x5c92409b, 0xa3aa2682, 0x4da97646, 0xbcfdec81, 0x25d5b61d, 0x20e1660d, 0x4b5214ed, 0x91aa596a,
                            0xb241415c, 0x88ec91a1, 0x2375e939, 0x981ad627, 0x4a54ee18, 0x13d98660, 0x9375c64d, 0x538d3b28, 0x4bf37ca7, 0x192b351e, 0x3cacf215,
                            0x3ecf3565, 0x50f5c0fc, 0xaafe3d4e, 0x6351b4f5, 0x1b800d4f, 0xfad73cdf, 0xe300e1d8, 0xb2cb5b04, 0xfb019702, 0xfb647f85, 0x375a7b74,
                            0xed6a6760, 0x45c54e76, 0x06524d79},
                    {0x48722ec4, 0x8a2694db, 0x3cf80478, 0xf9bc47ba, 0x76b258fb, 0xf71a1ec6, 0x841189df, 0x1a866461, 0x72b5488c, 0x71663983, 0xbda59407,
                            0xa2b68f85, 0x62dbd0aa, 0xe4966aa3, 0x32e0efaa, 0x71bb3699, 0x2eda14a6, 0x53f8917c, 0x874974ce, 0xe680bcca, 0x96a9c462, 0x399ca451,
                            0xc46616f5, 0xeee71114, 0x5878e472, 0x3a83c559, 0x54862a18, 0x82aea480, 0x492d0019, 0xd62a7027, 0x36655f50, 0xce412fdf, 0xc8136871,
                            0xd6cfe1d8, 0x121c9c91, 0x13abbf51, 0x3aaa7037, 0x9f6e7cb6, 0xae82c4c4, 0x55fdce32, 0xd8dd6bda, 0xd6ec4938, 0x6a5aee52, 0x52c8a764,
                            0xa6a85297, 0x5131de9e, 0x396a6599, 0xe27b1100, 0xe68588d3, 0x7b89a612, 0xad48a7a4, 0xfd205673, 0x81807089, 0x239d2d38, 0x39518df3,
                            0x256f3f14, 0x5c65e7b8, 0x64caebdc, 0xd8d694b6, 0xb4a87da3, 0xa651881e, 0xca1d252d, 0x993a3ddc, 0x14f9a54d, 0x6b14d2ff, 0xbbed03bb,
                            0x8d12bc03, 0x6cce455d, 0x613d6487, 0x6d04ce6a, 0xc2f4c84c, 0x306d8ff2, 0x584a9847, 0x68902fc5, 0x70af1a4f, 0x3ab4cb98, 0xe8be4453,
                            0x7e95d355, 0x84b0f371, 0x4c5ccb52, 0xdd6d029c, 0xafa47124, 0x71aabf91, 0xd3407f95, 0xe7fa3a9c, 0x4f634405, 0x0cbf2cb7, 0x0192ff17,
                            0x296959dd, 0x9e4d34d5, 0xfd9a4286, 0xac7b6933, 0x4650f585, 0x168af40d, 0x73816119, 0x5542d96d, 0x99047276, 0x1b5bbe67, 0x01a8209e,
                            0x6f9db32e, 0xd762bbd1, 0x299a3804, 0x87abe66d, 0xd479eeaa, 0x79928f4e, 0x3937ffbc, 0x3c8e83ca, 0x2a8f9347, 0x4d2324d3, 0xf0183dda,
                            0x9fbedb15, 0xac365889, 0xf1be552c, 0xa4b32d5a, 0xdc77fff3, 0x9d516da8, 0x7f3c347c, 0x39e8479f, 0x9e869687, 0x6a160347, 0x49ab7403,
                            0x830d31c7, 0x11311354, 0x79e6cc69, 0x35b25caa, 0x398af9aa, 0x02ef4356, 0xb5ecba53, 0x666d6c8b, 0x8836b3ae, 0x23b9fc98, 0x0cc8e3d0,
                            0x3ad594e1, 0xb124529d, 0xe059c1de, 0xfa88e0d9, 0xba117846, 0x1782a65a, 0xee9f80f9, 0xbc9aec55, 0x88aec1d4, 0x9c3907fa, 0x92b7b5bf,
                            0x464acbf4, 0xbbbd04a8, 0xf0e966bf, 0x14c5f971, 0x83018d49, 0xfaf4fc0a, 0x3b4639b2, 0x6b7e297d, 0xc0e9a807, 0x418713d3, 0x1a2b2361,
                            0x80850d90, 0xd515816e, 0x3deb48ea, 0x6bfe6aa1, 0x3680036c, 0x228e76ae, 0x78f16c87, 0xff4d85ea, 0x7d831974, 0xba962d6b, 0x4bae0b1d,
                            0xc0db431a, 0x04b46400, 0xcf427175, 0x244e321d, 0x1c8b1fc9, 0x63a2b794, 0x1939d9c6, 0xc92a530e, 0x21a8e5ad, 0x28050194, 0x3b106223,
                            0xb21e2ce1, 0x7ae71fe4, 0x7f7759f0, 0x0329c8f4, 0xd09f6b37, 0x897e12a5, 0x4103c4b1, 0x56520dae, 0x5d7391aa, 0x7ac9f12d, 0xeac6b834,
                            0x99f8f6a8, 0x2867867a, 0xff6f3343, 0x3167097a, 0x38432d1d, 0x108377f8, 0xfd8e0d5f, 0x25e15692, 0xf00d40f9, 0x1f1276f3, 0xb748c8cd,
                            0x6dbb9d9c, 0x99ab7ceb, 0xa4a9784f, 0xcb4b2535, 0xb3eb5ca7, 0xd3a09e75, 0x90f3ee7e, 0x28ef2a57, 0xbdb643a2, 0x1112ab10, 0x546b1af2,
                            0x8c41e90d, 0x0f5fcd88, 0x6f259f40, 0x34b33966, 0x5f3558d7, 0xfff36f0b, 0xa3459449, 0xdcecbce1, 0x69ff2bf7, 0x7525e1da, 0x24c9de72,
                            0xea9626b1, 0x87c7385d, 0x15e4211e, 0x9f7ef269, 0xfed018d1, 0x7632076c, 0x8d4f0157, 0x10c1205a, 0x65db0e00, 0x813f0e8b, 0xbafea255,
                            0xb47e6663, 0x2a0eba78, 0xf66b3783, 0xfff1db48, 0x47997f03, 0x3a49e877, 0x4536a0b5, 0x89b0738f, 0xf5758b5e, 0x1d277388, 0xf5db28e8,
                            0xb4ef0add, 0x776fed12, 0x045b614a, 0xc95f47ae, 0x13a1d602, 0x217d6338, 0xc509d080, 0x006789de, 0xd891cccc, 0xb02f9980, 0x67f00301,
                            0xafc87999, 0x043d8fbd, 0xb32d6061}};
}
