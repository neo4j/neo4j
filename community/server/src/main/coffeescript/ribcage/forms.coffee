###
Copyright (c) 2002-2011 "Neo Technology,"
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
  ['ribcage/View'
   'ribcage/security/HtmlEscaper'
   'ribcage/ui/Nano'
   'lib/colorpicker'
   'lib/rgbcolor'],
  (View, HtmlEscaper,Nano) ->
    exports = {}
    
    exports.ModelForm = class ModelForm extends View

      fields : {}

      initialize : (opts)->
        @instance = opts.instance
        
      render : =>
        wrap = $("<ul></ul>")
        model = @model
        for key, field of @fields
          do (key)->
            valChanger = (newValue) =>
              model.set key, newValue
            wrap.append field.renderLi(model.get(key), valChanger)
        
        $(@el).html(wrap)
        return this
        
    #
    # FIELDS
    #
    
    exports.Field = class Field
      
      constructor : (@label) -> 
      
      renderLi : (value, triggerValueChange) ->
        @renderWithTemplate "<li>{label}: {input}</li>", value, triggerValueChange
      
      renderWithTemplate : (tpl, value, triggerValueChange) ->
        r = $ Nano.compile tpl, {
          label : @label
          input : "<div class='PLACEHOLDER'></div>"
        }
        
        $('.PLACEHOLDER', r).replaceWith(@renderElement(value, triggerValueChange))
        r
    
    exports.TextField = class TextField extends Field

      renderElement : (value, triggerValueChange) =>
        return "<input type='text' />"
        
    exports.ColorField = class ColorField extends Field
      
      renderElement : (value, triggerValueChange) =>
        el = $ "<div class='colorpicker-input' style='background-color: #{value}'></div>"
        el.ColorPicker
          onChange: (hsb, hex, rgb) ->
            $(el).css 'background-color' : "##{hex}"
          onBeforeShow: () ->
            $(el).ColorPickerSetColor $(el).css 'background-color'
          onHide: (hsb, hex, rgb) ->
            color = new RGBColor($(el).css 'background-color')
            triggerValueChange color.toRGB()
        el
        
    return exports
          
)
