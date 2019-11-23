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
package org.neo4j.values.storable;

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.values.Comparison;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

class PointTest
{
    @Test
    void cartesianShouldEqualItself()
    {
        assertEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, 1.0, 2.0 ) );
        assertEqual( pointValue( Cartesian, -1.0, 2.0 ), pointValue( Cartesian, -1.0, 2.0 ) );
        assertEqual( pointValue( Cartesian, -1.0, -2.0 ), pointValue( Cartesian, -1.0, -2.0 ) );
        assertEqual( pointValue( Cartesian, 0.0, 0.0 ), pointValue( Cartesian, 0.0, 0.0 ) );
    }

    @Test
    void cartesianShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, 3.0, 4.0 ) );
        assertNotEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, -1.0, 2.0 ) );
    }

    @Test
    void geographicShouldEqualItself()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, 1.0, 2.0 ) );
        assertEqual( pointValue( WGS84, -1.0, 2.0 ), pointValue( WGS84, -1.0, 2.0 ) );
        assertEqual( pointValue( WGS84, -1.0, -2.0 ), pointValue( WGS84, -1.0, -2.0 ) );
        assertEqual( pointValue( WGS84, 0.0, 0.0 ), pointValue( WGS84, 0.0, 0.0 ) );
    }

    @Test
    void geographicShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, 3.0, 4.0 ) );
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, -1.0, 2.0 ) );
    }

    @Test
    void geographicShouldNotEqualCartesian()
    {
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( Cartesian, 1.0, 2.0 ) );
    }

    @Test
    void geometricInvalid2DPointsShouldBehave()
    {
        // we wrap around for x [-180,180]
        // we fail on going over or under [-90,90] for y

        // valid ones for x
        assertArrayEquals( pointValue( WGS84, 0, 0 ).coordinate(), new double[]{0, 0} );
        assertArrayEquals( pointValue( WGS84, 180, 0 ).coordinate(), new double[]{180, 0} );
        assertArrayEquals( pointValue( WGS84, -180, 0 ).coordinate(), new double[]{-180, 0} );

        // valid ones for x that should wrap around
        assertArrayEquals( pointValue( WGS84, 190, 0 ).coordinate(), new double[]{-170, 0} );
        assertArrayEquals( pointValue( WGS84, -190, 0 ).coordinate(), new double[]{170, 0} );
        assertArrayEquals( pointValue( WGS84, 360, 0 ).coordinate(), new double[]{0, 0} );
        assertArrayEquals( pointValue( WGS84, -360, 0 ).coordinate(), new double[]{0, 0} );
        assertArrayEquals( pointValue( WGS84, 350, 0 ).coordinate(), new double[]{-10, 0} );
        assertArrayEquals( pointValue( WGS84, -350, 0 ).coordinate(), new double[]{10, 0} );
        assertArrayEquals( pointValue( WGS84, 370, 0 ).coordinate(), new double[]{10, 0} );
        assertArrayEquals( pointValue( WGS84, -370, 0 ).coordinate(), new double[]{-10, 0} );
        assertArrayEquals( pointValue( WGS84, 540, 0 ).coordinate(), new double[]{180, 0} );
        assertArrayEquals( pointValue( WGS84, -540, 0 ).coordinate(), new double[]{-180, 0} );

        // valid ones for y
        assertArrayEquals( pointValue( WGS84, 0, 90 ).coordinate(), new double[]{0, 90} );
        assertArrayEquals( pointValue( WGS84, 0, -90 ).coordinate(), new double[]{0, -90} );

        // invalid ones for y
        assertThrows( InvalidArgumentException.class, () -> pointValue( WGS84, 0, 91 ),
                "Cannot create WGS84 point with invalid coordinate: [0.0, 91.0]. Valid range for Y coordinate is [-90, 90]." );
        assertThrows( InvalidArgumentException.class, () -> pointValue( WGS84, 0, -91 ),
                "Cannot create WGS84 point with invalid coordinate: [0.0, -91.0]. Valid range for Y coordinate is [-90, 90]." );
    }

    @Test
    void geometricInvalid3DPointsShouldBehave()
    {
        // we wrap around for x [-180,180]
        // we fail on going over or under [-90,90] for y
        // we accept all values for z

        // valid ones for x
        assertArrayEquals( pointValue( WGS84_3D, 0, 0, 0 ).coordinate(), new double[]{0, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, 180, 0, 0 ).coordinate(), new double[]{180, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, -180, 0, 0 ).coordinate(), new double[]{-180, 0, 0} );

        // valid ones for x that should wrap around
        assertArrayEquals( pointValue( WGS84_3D, 190, 0, 0 ).coordinate(), new double[]{-170, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, -190, 0, 0 ).coordinate(), new double[]{170, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, 360, 0, 0 ).coordinate(), new double[]{0, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, -360, 0, 0 ).coordinate(), new double[]{0, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, 350, 0, 0 ).coordinate(), new double[]{-10, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, -350, 0, 0 ).coordinate(), new double[]{10, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, 370, 0, 0 ).coordinate(), new double[]{10, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, -370, 0, 0 ).coordinate(), new double[]{-10, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, 540, 0, 0 ).coordinate(), new double[]{180, 0, 0} );
        assertArrayEquals( pointValue( WGS84_3D, -540, 0, 0 ).coordinate(), new double[]{-180, 0, 0} );

        // valid ones for y
        assertArrayEquals( pointValue( WGS84_3D, 0, 90, 0 ).coordinate(), new double[]{0, 90, 0} );
        assertArrayEquals( pointValue( WGS84_3D, 0, -90, 0 ).coordinate(), new double[]{0, -90, 0} );

        // invalid ones for y
        assertThrows( InvalidArgumentException.class, () -> pointValue( WGS84_3D, 0, 91, 0 ),
                "Cannot create WGS84 point with invalid coordinate: [0.0, 91.0, 0.0]. Valid range for Y coordinate is [-90, 90]." );
        assertThrows( InvalidArgumentException.class, () -> pointValue( WGS84_3D, 0, -91, 0 ),
                "Cannot create WGS84 point with invalid coordinate: [0.0, -91.0, 0.0]. Valid range for Y coordinate is [-90, 90]." );
    }

    @Test
    void shouldHaveValueGroup()
    {
        assertNotNull( pointValue( Cartesian, 1, 2 ).valueGroup() );
        assertNotNull( pointValue( WGS84, 1, 2 ).valueGroup() );
    }

    //-------------------------------------------------------------
    // Comparison tests

    @Test
    public void shouldCompareTwoPoints()
    {
        assertThat( pointValue( Cartesian, 1, 2 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Two identical points should be equal" ).isEqualTo( 0 );
        assertThat( pointValue( Cartesian, 1, 2 ).compareTo( pointValue( WGS84, 1, 2 ) ) ).as( "Different CRS should compare CRS codes" ).isEqualTo( 1 );
        assertThat( pointValue( Cartesian, 2, 3 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Point greater on both dimensions is greater" ).isEqualTo(
                1 );
        assertThat( pointValue( Cartesian, 2, 2 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Point greater on first dimensions is greater" ).isEqualTo(
                1 );
        assertThat( pointValue( Cartesian, 1, 3 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Point greater on second dimensions is greater" ).isEqualTo(
                1 );
        assertThat( pointValue( Cartesian, 0, 1 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Point smaller on both dimensions is smaller" ).isEqualTo(
                -1 );
        assertThat( pointValue( Cartesian, 0, 2 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Point smaller on first dimensions is smaller" ).isEqualTo(
                -1 );
        assertThat( pointValue( Cartesian, 1, 1 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as( "Point smaller on second dimensions is smaller" ).isEqualTo(
                -1 );
        assertThat( pointValue( Cartesian, 2, 1 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point greater on first and smaller on second dimensions is greater" ).isEqualTo( 1 );
        assertThat( pointValue( Cartesian, 0, 3 ).compareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point smaller on first and greater on second dimensions is smaller" ).isEqualTo( -1 );
    }

    @Test
    public void shouldTernaryCompareTwoPoints()
    {
        assertThat( pointValue( Cartesian, 1, 2 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Two identical points should be equal" ).isEqualTo( Comparison.EQUAL );
        assertThat( pointValue( Cartesian, 1, 2 ).unsafeTernaryCompareTo( pointValue( WGS84, 1, 2 ) ) ).as( "Different CRS should be incomparable" ).isEqualTo(
                Comparison.UNDEFINED );
        assertThat( pointValue( Cartesian, 2, 3 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point greater on both dimensions is greater" ).isEqualTo( Comparison.GREATER_THAN );
        assertThat( pointValue( Cartesian, 2, 2 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point greater on first dimensions is >=" ).isEqualTo( Comparison.GREATER_THAN_AND_EQUAL );
        assertThat( pointValue( Cartesian, 1, 3 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point greater on second dimensions is >=" ).isEqualTo( Comparison.GREATER_THAN_AND_EQUAL );
        assertThat( pointValue( Cartesian, 0, 1 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point smaller on both dimensions is smaller" ).isEqualTo( Comparison.SMALLER_THAN );
        assertThat( pointValue( Cartesian, 0, 2 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point smaller on first dimensions is <=" ).isEqualTo( Comparison.SMALLER_THAN_AND_EQUAL );
        assertThat( pointValue( Cartesian, 1, 1 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point smaller on second dimensions is <=" ).isEqualTo( Comparison.SMALLER_THAN_AND_EQUAL );
        assertThat( pointValue( Cartesian, 2, 1 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point greater on first and smaller on second dimensions is UNDEFINED" ).isEqualTo( Comparison.UNDEFINED );
        assertThat( pointValue( Cartesian, 0, 3 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ) ).as(
                "Point smaller on first and greater on second dimensions is UNDEFINED" ).isEqualTo( Comparison.UNDEFINED );
    }

    @Test
    public void shouldComparePointWithin()
    {
        // Edge cases
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, null, false ) ).as( "Always within no bounds" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( WGS84, 1, 2 ), true, null, false ) ).as(
                "Different CRS for lower bound should be undefined" ).isEqualTo( null );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( WGS84, 1, 2 ), true ) ).as(
                "Different CRS for upper bound should be undefined" ).isEqualTo( null );

        // Lower bound
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, null, false ) ).as(
                "Within same lower bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, null, false ) ).as(
                "Not within same lower bound if not inclusive" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), true, null, false ) ).as(
                "Within smaller lower bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, null, false ) ).as(
                "Within smaller lower bound if not inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), true, null, false ) ).as(
                "Within partially smaller lower bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), false, null, false ) ).as(
                "Not within partially smaller lower bound if not inclusive" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 2, 1 ), false, null, false ) ).as(
                "Invalid if lower bound both greater and less than" ).isEqualTo( null );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 2, 1 ), true, null, false ) ).as(
                "Invalid if lower bound both greater and less than even when inclusive" ).isEqualTo( null );

        // Upper bound
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Within same upper bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 1, 2 ), false ) ).as(
                "Not within same upper bound if not inclusive" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 3 ), true ) ).as(
                "Within larger upper bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 3 ), false ) ).as(
                "Within larger upper bound if not inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 2 ), true ) ).as(
                "Within partially larger upper bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 2 ), false ) ).as(
                "Not within partially larger upper bound if not inclusive" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 1 ), false ) ).as(
                "Invalid if upper bound both greater and less than" ).isEqualTo( null );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 1 ), true ) ).as(
                "Invalid if upper bound both greater and less than even when inclusive" ).isEqualTo( null );

        // Lower and upper bounds invalid
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 2, 1 ), true, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Undefined if lower bound greater than upper bound" ).isEqualTo( null );

        // Lower and upper bounds equal
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), false ) ).as(
                "Not within same bounds if inclusive on lower" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Not within same bounds if inclusive on upper" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Within same bounds if inclusive on both" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), false ) ).as(
                "Not within same bounds if not inclusive" ).isEqualTo( false );

        // Lower and upper bounds define 0x1 range
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), false ) ).as(
                "Not within same bounds if inclusive on lower" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Not within same bounds if inclusive on upper" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Within same bounds if inclusive on both" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), false ) ).as(
                "Not within same bounds if not inclusive" ).isEqualTo( false );

        // Lower and upper bounds define 1x1 range
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), true, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Within smaller lower bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, pointValue( Cartesian, 1, 2 ), true ) ).as(
                "Within smaller lower bound if inclusive on upper" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, pointValue( Cartesian, 1, 2 ), false ) ).as(
                "Not within smaller lower bound if not inclusive" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), true, pointValue( Cartesian, 2, 3 ), false ) ).as(
                "Within partially smaller lower bound if inclusive" ).isEqualTo( true );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), false, pointValue( Cartesian, 2, 3 ), false ) ).as(
                "Not within partially smaller lower bound if not inclusive" ).isEqualTo( false );
        assertThat( pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, pointValue( Cartesian, 2, 3 ), false ) ).as(
                "Within wider bounds" ).isEqualTo( true );
    }

    @Test
    public void shouldComparePointWithinTwoBoundsExhaustive()
    {
        for ( int minx = -5; minx < 5; minx++ )
        {
            for ( int maxx = -5; maxx < 5; maxx++ )
            {
                for ( int miny = -5; miny < 5; miny++ )
                {
                    for ( int maxy = -5; maxy < 5; maxy++ )
                    {
                        PointValue min = pointValue( Cartesian, minx, miny );
                        PointValue max = pointValue( Cartesian, maxx, maxy );
                        for ( int x = -5; x < 5; x++ )
                        {
                            for ( int y = -5; y < 5; y++ )
                            {
                                PointValue point = pointValue( Cartesian, x, y );
                                boolean invalidRange = minx > maxx || miny > maxy;
                                boolean undefinedMin = x > minx && y < miny || y > miny && x < minx;
                                boolean undefinedMax = x < maxx && y > maxy || y < maxy && x > maxx;
                                Boolean ii = (invalidRange || undefinedMin || undefinedMax) ? null : x >= minx && y >= miny && x <= maxx && y <= maxy;
                                Boolean ix = (invalidRange || undefinedMin || undefinedMax) ? null : x >= minx && y >= miny && x < maxx && y < maxy;
                                Boolean xi = (invalidRange || undefinedMin || undefinedMax) ? null : x > minx && y > miny && x <= maxx && y <= maxy;
                                Boolean xx = (invalidRange || undefinedMin || undefinedMax) ? null : x > minx && y > miny && x < maxx && y < maxy;
                                // inclusive:inclusive
                                assertThat( point.withinRange( min, true, max, true ) ).as(
                                        "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, true, {" + maxx + "," + maxy + "}, true" ).isEqualTo(
                                        ii );
                                // inclusive:exclusive
                                assertThat( point.withinRange( min, true, max, false ) ).as(
                                        "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, true, {" + maxx + "," + maxy + "}, false" ).isEqualTo(
                                        ix );
                                // exclusive:inclusive
                                assertThat( point.withinRange( min, false, max, true ) ).as(
                                        "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, false, {" + maxx + "," + maxy + "}, true" ).isEqualTo(
                                        xi );
                                // exclusive:exclusive
                                assertThat( point.withinRange( min, false, max, false ) ).as(
                                        "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, false, {" + maxx + "," + maxy + "}, false" ).isEqualTo(
                                        xx );
                            }
                        }
                    }
                }
            }
        }
    }

        //-------------------------------------------------------------
    // Parser tests

    @Test
    void shouldBeAbleToParsePoints()
    {
        assertEqual( pointValue( WGS84, 13.2, 56.7 ),
                PointValue.parse( "{latitude: 56.7, longitude: 13.2}" ) );
        assertEqual( pointValue( WGS84, -74.0060, 40.7128 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, crs: 'wgs-84'}" ) ); // - explicitly WGS84
        assertEqual( pointValue( Cartesian, -21, -45.3 ),
                PointValue.parse( "{x: -21, y: -45.3}" ) ); // - default to cartesian 2D
        assertEqual( pointValue( WGS84, -21, -45.3 ),
                PointValue.parse( "{x: -21, y: -45.3, srid: 4326}" ) ); // - explicitly set WGS84 by SRID
        assertEqual( pointValue( Cartesian, 17, -52.8 ),
                PointValue.parse( "{x: 17, y: -52.8, crs: 'cartesian'}" ) ); // - explicit cartesian 2D
        assertEqual( pointValue( WGS84_3D, 13.2, 56.7, 123.4 ),
                PointValue.parse( "{latitude: 56.7, longitude: 13.2, height: 123.4}" ) ); // - defaults to WGS84-3D
        assertEqual( pointValue( WGS84_3D, 13.2, 56.7, 123.4 ),
                PointValue.parse( "{latitude: 56.7, longitude: 13.2, z: 123.4}" ) ); // - defaults to WGS84-3D
        assertEqual( pointValue( WGS84_3D, -74.0060, 40.7128, 567.8 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs: 'wgs-84-3D'}" ) ); // - explicitly WGS84-3D
        assertEqual( pointValue( Cartesian_3D, -21, -45.3, 7.2 ),
                PointValue.parse( "{x: -21, y: -45.3, z: 7.2}" ) ); // - default to cartesian 3D
        assertEqual( pointValue( Cartesian_3D, 17, -52.8, -83.1 ),
                PointValue.parse( "{x: 17, y: -52.8, z: -83.1, crs: 'cartesian-3D'}" ) ); // - explicit cartesian 3D
    }

    @Test
    void shouldBeAbleToParsePointWithUnquotedCrs()
    {
        assertEqual( pointValue( WGS84_3D, -74.0060, 40.7128, 567.8 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}" ) ); // - explicitly WGS84-3D, without quotes
    }

    @Test
    void shouldBeAbleToParsePointThatOverridesHeaderInformation()
    {
        String headerInformation = "{crs:wgs-84}";
        String data = "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}";

        PointValue expected = PointValue.parse( data );
        PointValue actual = PointValue.parse( data, PointValue.parseHeaderInformation( headerInformation ) );

        assertEqual( expected, actual );
        assertEquals( "wgs-84-3d", actual.getCoordinateReferenceSystem().getName().toLowerCase() );
    }

    @Test
    void shouldBeAbleToParseIncompletePointWithHeaderInformation()
    {
        String headerInformation = "{latitude: 40.7128}";
        String data = "{longitude: -74.0060, height: 567.8, crs:wgs-84-3D}";

        assertThrows( InvalidArgumentException.class, () -> PointValue.parse( data ) );

        // this should work
        PointValue.parse( data, PointValue.parseHeaderInformation( headerInformation ) );
    }

    @Test
    void shouldBeAbleToParseWeirdlyFormattedPoints()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( " \t\n { latitude : 2.0  ,longitude :1.0  } \t" ) );
        // TODO: Should some/all of these fail?
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( " \t\n { latitude : 2.0  ,longitude :1.0 , } \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { x :+.2e-7,y: -1.0E07 , } \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { x :+.2e-7,y: -1.0E07 , garbage} \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { gar ba ge,x :+.2e-7,y: -1.0E07} \t" ) );
    }

    @Test
    void shouldNotBeAbleToParsePointsWithConflictingDuplicateFields()
    {
        assertThat( assertCannotParse( "{latitude: 2.0, longitude: 1.0, latitude: 3.0}" ).getMessage() ).contains( "Duplicate field" );
        assertThat( assertCannotParse( "{latitude: 2.0, longitude: 1.0, latitude: 3.0}" ).getMessage() ).contains( "Duplicate field" );
        assertThat( assertCannotParse( "{crs: 'cartesian', x: 2.0, x: 1.0, y: 3}" ).getMessage() ).contains( "Duplicate field" );
        assertThat( assertCannotParse( "{crs: 'invalid crs', x: 1.0, y: 3, crs: 'cartesian'}" ).getMessage() ).contains( "Duplicate field" );
    }

    @Test
    void shouldNotBeAbleToParseIncompletePoints()
    {
        assertCannotParse( "{latitude: 56.7, longitude:}" );
        assertCannotParse( "{latitude: 56.7}" );
        assertCannotParse( "{}" );
        assertCannotParse( "{only_a_key}" );
        assertCannotParse( "{crs:'WGS-84'}" );
        assertCannotParse( "{a:a}" );
        assertCannotParse( "{ : 2.0, x : 1.0 }" );
        assertCannotParse( "x:1,y:2" );
        assertCannotParse( "{x:1,y:2,srid:-9}" );
        assertCannotParse( "{x:1,y:'2'}" );
        assertCannotParse( "{crs:WGS-84 , lat:1, y:2}" );
    }

    private InvalidArgumentException assertCannotParse( String text )
    {
        return assertThrows( InvalidArgumentException.class, () -> PointValue.parse( text ) );
    }
}
