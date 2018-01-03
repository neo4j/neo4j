###
Copyright (c) 2002-2018 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['ribcage/View'], 
  (View) ->
    class ImagePicker extends View
      
      tagName : "ul"
      
      constructor : (@imgUrls, @cols=8) ->
        super()
        @el = $ @el
        @el.addClass "image-picker"
        @el.addClass "grid"
      
      render : () ->
        @el.html("")
        for i in [0...@imgUrls.length]
          if i % @cols == 0
            ul = $ '<ul></ul>'
            li = $ '<li></li>'
            li.append ul
            @el.append li
          url = @imgUrls[i]
          li = $ "<li><div class='imagepicker-image' style='background:url(#{url}) no-repeat center center;'><div/></li>"
          
          do (url) =>
            li.click () => @imageClicked url
          
          ul.append li
        return this
        
      imageClicked : (url) =>
        @trigger "image:clicked", {url:url}
      
    _(ImagePicker.prototype).extend(Backbone.Events)
    ImagePicker
    
        
)
