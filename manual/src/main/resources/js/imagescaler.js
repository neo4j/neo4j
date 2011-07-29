/* Smart Image Scaling
 * Makes sure images are never scaled to more than 100%
 * while preserving image scaling as much as possible.
 */

jQuery( window ).load(  function()
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

