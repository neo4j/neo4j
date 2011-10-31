/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/* Smart Image Scaling
 * Makes sure images are never scaled to more than 100%
 * while preserving image scaling as much as possible.
 */

jQuery( document ).ready(  function()
{
  setImageSizes( jQuery );
});

function setImageSizes( $ )
{
  $( "span.inlinemediaobject > img[width]" ).each( function()
  {
    var img = this;
    var width = $( this ).parent().width();
    if ( img.naturalWidth && width > img.naturalWidth )
    {
      removeWidth( img );
    }
    else if ( img.realWidth && width > img.realWidth )
    {
      removeWidth( img );
    }
    else
    {
      $("<img />")
        .attr( "src", img.getAttribute( "src" ) + "?" + new Date().getTime() )
        .load( function( )
        {
          img.realWidth = this.width;
          if ( width > this.width )
          {
            removeWidth( img );
          }
        });
    }
  });
}

function resetImageSizes( $ )
{
  $( "span.inlinemediaobject > img" ).each( function()
  {
    if ( this.originalWidth )
    {
      this.setAttribute( "width", this.originalWidth );
    }
  });
  setImageSizes( $ );
}

jQuery( window ).resize( function()
{
  resetImageSizes( jQuery );
});

function removeWidth( image )
{
  if ( ! image.originalWidth )
  {
    image.originalWidth = image.getAttribute( "width" );
  }
  image.removeAttribute( "width" );
}

