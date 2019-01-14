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

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.neo4j.values.Comparison;
import org.neo4j.values.utils.InvalidValuesArgumentException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class PointTest
{
    @Test
    public void cartesianShouldEqualItself()
    {
        assertEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, 1.0, 2.0 ) );
        assertEqual( pointValue( Cartesian, -1.0, 2.0 ), pointValue( Cartesian, -1.0, 2.0 ) );
        assertEqual( pointValue( Cartesian, -1.0, -2.0 ), pointValue( Cartesian, -1.0, -2.0 ) );
        assertEqual( pointValue( Cartesian, 0.0, 0.0 ), pointValue( Cartesian, 0.0, 0.0 ) );
    }

    @Test
    public void cartesianShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, 3.0, 4.0 ) );
        assertNotEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, -1.0, 2.0 ) );
    }

    @Test
    public void geographicShouldEqualItself()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, 1.0, 2.0 ) );
        assertEqual( pointValue( WGS84, -1.0, 2.0 ), pointValue( WGS84, -1.0, 2.0 ) );
        assertEqual( pointValue( WGS84, -1.0, -2.0 ), pointValue( WGS84, -1.0, -2.0 ) );
        assertEqual( pointValue( WGS84, 0.0, 0.0 ), pointValue( WGS84, 0.0, 0.0 ) );
    }

    @Test
    public void geographicShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, 3.0, 4.0 ) );
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, -1.0, 2.0 ) );
    }

    @Test
    public void geographicShouldNotEqualCartesian()
    {
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( Cartesian, 1.0, 2.0 ) );
    }

    @Test
    public void shouldHaveValueGroup()
    {
        assertTrue( pointValue( Cartesian, 1, 2 ).valueGroup() != null );
        assertTrue( pointValue( WGS84, 1, 2 ).valueGroup() != null );
    }

    //-------------------------------------------------------------
    // Comparison tests

    @Test
    public void shouldCompareTwoPoints()
    {
        assertThat( "Two identical points should be equal", pointValue( Cartesian, 1, 2 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( 0 ) );
        assertThat( "Different CRS should compare CRS codes", pointValue( Cartesian, 1, 2 ).compareTo( pointValue( WGS84, 1, 2 ) ),
                equalTo( Cartesian.getCode() - WGS84.getCode() ) );
        assertThat( "Point greater on both dimensions is greater", pointValue( Cartesian, 2, 3 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( 1 ) );
        assertThat( "Point greater on first dimensions is greater", pointValue( Cartesian, 2, 2 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( 1 ) );
        assertThat( "Point greater on second dimensions is greater", pointValue( Cartesian, 1, 3 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( 1 ) );
        assertThat( "Point smaller on both dimensions is smaller", pointValue( Cartesian, 0, 1 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( -1 ) );
        assertThat( "Point smaller on first dimensions is smaller", pointValue( Cartesian, 0, 2 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( -1 ) );
        assertThat( "Point smaller on second dimensions is smaller", pointValue( Cartesian, 1, 1 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( -1 ) );
        assertThat( "Point greater on first and smaller on second dimensions is greater",
                pointValue( Cartesian, 2, 1 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( 1 ) );
        assertThat( "Point smaller on first and greater on second dimensions is smaller",
                pointValue( Cartesian, 0, 3 ).compareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( -1 ) );
    }

    @Test
    public void shouldTernaryCompareTwoPoints()
    {
        assertThat( "Two identical points should be equal", pointValue( Cartesian, 1, 2 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.EQUAL ) );
        assertThat( "Different CRS should be incomparable", pointValue( Cartesian, 1, 2 ).unsafeTernaryCompareTo( pointValue( WGS84, 1, 2 ) ),
                equalTo( Comparison.UNDEFINED ) );
        assertThat( "Point greater on both dimensions is greater", pointValue( Cartesian, 2, 3 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.GREATER_THAN ) );
        assertThat( "Point greater on first dimensions is >=", pointValue( Cartesian, 2, 2 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.GREATER_THAN_AND_EQUAL ) );
        assertThat( "Point greater on second dimensions is >=", pointValue( Cartesian, 1, 3 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.GREATER_THAN_AND_EQUAL ) );
        assertThat( "Point smaller on both dimensions is smaller", pointValue( Cartesian, 0, 1 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.SMALLER_THAN ) );
        assertThat( "Point smaller on first dimensions is <=", pointValue( Cartesian, 0, 2 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.SMALLER_THAN_AND_EQUAL ) );
        assertThat( "Point smaller on second dimensions is <=", pointValue( Cartesian, 1, 1 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ),
                equalTo( Comparison.SMALLER_THAN_AND_EQUAL ) );
        assertThat( "Point greater on first and smaller on second dimensions is UNDEFINED",
                pointValue( Cartesian, 2, 1 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( Comparison.UNDEFINED ) );
        assertThat( "Point smaller on first and greater on second dimensions is UNDEFINED",
                pointValue( Cartesian, 0, 3 ).unsafeTernaryCompareTo( pointValue( Cartesian, 1, 2 ) ), equalTo( Comparison.UNDEFINED ) );
    }

    @Test
    public void shouldComparePointWithin()
    {
        // Edge cases
        assertThat( "Always within no bounds", pointValue( Cartesian, 1, 2 ).withinRange( null, false, null, false ), equalTo( true ) );
        assertThat( "Different CRS for lower bound should be undefined",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( WGS84, 1, 2 ), true, null, false ), equalTo( null ) );
        assertThat( "Different CRS for upper bound should be undefined",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( WGS84, 1, 2 ), true ), equalTo( null ) );

        // Lower bound
        assertThat( "Within same lower bound if inclusive", pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, null, false ),
                equalTo( true ) );
        assertThat( "Not within same lower bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, null, false ), equalTo( false ) );
        assertThat( "Within smaller lower bound if inclusive", pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), true, null, false ),
                equalTo( true ) );
        assertThat( "Within smaller lower bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, null, false ), equalTo( true ) );
        assertThat( "Within partially smaller lower bound if inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), true, null, false ), equalTo( true ) );
        assertThat( "Not within partially smaller lower bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), false, null, false ), equalTo( false ) );
        assertThat( "Invalid if lower bound both greater and less than",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 2, 1 ), false, null, false ), equalTo( null ) );
        assertThat( "Invalid if lower bound both greater and less than even when inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 2, 1 ), true, null, false ), equalTo( null ) );

        // Upper bound
        assertThat( "Within same upper bound if inclusive", pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 1, 2 ), true ),
                equalTo( true ) );
        assertThat( "Not within same upper bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 1, 2 ), false ), equalTo( false ) );
        assertThat( "Within larger upper bound if inclusive", pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 3 ), true ),
                equalTo( true ) );
        assertThat( "Within larger upper bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 3 ), false ), equalTo( true ) );
        assertThat( "Within partially larger upper bound if inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 2 ), true ), equalTo( true ) );
        assertThat( "Not within partially larger upper bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 2 ), false ), equalTo( false ) );
        assertThat( "Invalid if upper bound both greater and less than",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 1 ), false ), equalTo( null ) );
        assertThat( "Invalid if upper bound both greater and less than even when inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( null, false, pointValue( Cartesian, 2, 1 ), true ), equalTo( null ) );

        // Lower and upper bounds invalid
        assertThat( "Undefined if lower bound greater than upper bound",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 2, 1 ), true, pointValue( Cartesian, 1, 2 ), true ), equalTo( null) );

        // Lower and upper bounds equal
        assertThat( "Not within same bounds if inclusive on lower",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), false ), equalTo( false ) );
        assertThat( "Not within same bounds if inclusive on upper",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), true ), equalTo( false ) );
        assertThat( "Within same bounds if inclusive on both",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), true ), equalTo( true ) );
        assertThat( "Not within same bounds if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), false ), equalTo( false ) );

        // Lower and upper bounds define 0x1 range
        assertThat( "Not within same bounds if inclusive on lower",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), false ), equalTo( false ) );
        assertThat( "Not within same bounds if inclusive on upper",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), true ), equalTo( false ) );
        assertThat( "Within same bounds if inclusive on both",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), true, pointValue( Cartesian, 1, 2 ), true ), equalTo( true ) );
        assertThat( "Not within same bounds if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 2 ), false, pointValue( Cartesian, 1, 2 ), false ), equalTo( false ) );

        // Lower and upper bounds define 1x1 range
        assertThat( "Within smaller lower bound if inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), true, pointValue( Cartesian, 1, 2 ), true ), equalTo( true ) );
        assertThat( "Within smaller lower bound if inclusive on upper",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, pointValue( Cartesian, 1, 2 ), true ), equalTo( true ) );
        assertThat( "Not within smaller lower bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, pointValue( Cartesian, 1, 2 ), false ), equalTo( false ) );
        assertThat( "Within partially smaller lower bound if inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), true, pointValue( Cartesian, 2, 3 ), false ), equalTo( true ) );
        assertThat( "Not within partially smaller lower bound if not inclusive",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 1, 1 ), false, pointValue( Cartesian, 2, 3 ), false ), equalTo( false ) );
        assertThat( "Within wider bounds",
                pointValue( Cartesian, 1, 2 ).withinRange( pointValue( Cartesian, 0, 1 ), false, pointValue( Cartesian, 2, 3 ), false ), equalTo( true ) );
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
                                assertThat( "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, true, {" + maxx + "," + maxy + "}, true",
                                        point.withinRange( min, true, max, true ), equalTo( ii ) );
                                // inclusive:exclusive
                                assertThat( "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, true, {" + maxx + "," + maxy + "}, false",
                                        point.withinRange( min, true, max, false ), equalTo( ix ) );
                                // exclusive:inclusive
                                assertThat( "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, false, {" + maxx + "," + maxy + "}, true",
                                        point.withinRange( min, false, max, true ), equalTo( xi ) );
                                // exclusive:exclusive
                                assertThat( "{" + x + "," + y + "}.withinRange({" + minx + "," + miny + "}, false, {" + maxx + "," + maxy + "}, false",
                                        point.withinRange( min, false, max, false ), equalTo( xx ) );
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
    public void shouldBeAbleToParsePoints()
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
    public void shouldBeAbleToParsePointWithUnquotedCrs()
    {
        assertEqual( pointValue( WGS84_3D, -74.0060, 40.7128, 567.8 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}" ) ); // - explicitly WGS84-3D, without quotes
    }

    @Test
    public void shouldBeAbleToParsePointThatOverridesHeaderInformation()
    {
        String headerInformation = "{crs:wgs-84}";
        String data = "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}";

        PointValue expected = PointValue.parse( data );
        PointValue actual = PointValue.parse( data, PointValue.parseHeaderInformation( headerInformation ) );

        assertEqual( expected, actual );
        assertEquals( "wgs-84-3d", actual.getCoordinateReferenceSystem().getName().toLowerCase() );
    }

    @Test
    public void shouldBeAbleToParseIncompletePointWithHeaderInformation()
    {
        String headerInformation = "{latitude: 40.7128}";
        String data = "{longitude: -74.0060, height: 567.8, crs:wgs-84-3D}";

        try
        {
            PointValue.parse( data ); // this shouldn't work
            fail( "Was able to parse point although latitude was missing" );
        }
        catch ( InvalidValuesArgumentException e )
        {
            // this is expected
        }

        // this should work
        PointValue.parse( data, PointValue.parseHeaderInformation( headerInformation ) );

    }

    @Test
    public void shouldBeAbleToParseWeirdlyFormattedPoints()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( " \t\n { latitude : 2.0  ,longitude :1.0  } \t" ) );
        // TODO: Should some/all of these fail?
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( " \t\n { latitude : 2.0  ,longitude :1.0 , } \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { x :+.2e-7,y: -1.0E07 , } \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { x :+.2e-7,y: -1.0E07 , garbage} \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { gar ba ge,x :+.2e-7,y: -1.0E07} \t" ) );
    }

    @Test
    public void shouldNotBeAbleToParsePointsWithConflictingDuplicateFields()
    {
        assertThat( assertCannotParse( "{latitude: 2.0, longitude: 1.0, latitude: 3.0}" ).getMessage(), CoreMatchers.containsString( "Duplicate field" ) );
        assertThat( assertCannotParse( "{latitude: 2.0, longitude: 1.0, latitude: 3.0}" ).getMessage(), CoreMatchers.containsString( "Duplicate field" ) );
        assertThat( assertCannotParse( "{crs: 'cartesian', x: 2.0, x: 1.0, y: 3}" ).getMessage(), CoreMatchers.containsString( "Duplicate field" ) );
        assertThat( assertCannotParse( "{crs: 'invalid crs', x: 1.0, y: 3, crs: 'cartesian'}" ).getMessage(),
                CoreMatchers.containsString( "Duplicate field" ) );
    }

    @Test
    public void shouldNotBeAbleToParseIncompletePoints()
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

    private InvalidValuesArgumentException assertCannotParse( String text )
    {
        PointValue value;
        try
        {
            value = PointValue.parse( text );
        }
        catch ( InvalidValuesArgumentException e )
        {
            return e;
        }
        throw new AssertionError( String.format( "'%s' parsed to %s", text, value ) );
    }
}
