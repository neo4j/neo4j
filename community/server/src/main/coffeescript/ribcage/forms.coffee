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

      createFields : () -> {}

      initialize : (opts)->
        @instance = opts.instance
        @fields = @createFields()
        
      render : =>
        wrap = $("<ul class='form'></ul>")
        for key, fieldset of @_getFieldSets()
          wrap.append fieldset.renderLi @model
        
        $(@el).html(wrap)
        return this
        
      validates : () ->
        for k, field of @fields
          if not field.validates() then return false
        return true
        
      _getFieldSets : () ->
        sets = 
          _default : new FieldSet  
        hasDefaultFieldset = false
        for key, fieldDef of @fields
          if fieldDef instanceof FieldSet
            sets[key] = fieldDef
          else
            hasDefaultFieldset = true
            sets._default.fields[key] = fieldDef
        
        if not hasDefaultFieldset
          delete sets._default
        return sets
       
    
    exports.FieldSet = class FieldSet 
    
      constructor : (@label="", @fields={}) ->
        if not _(@label).isString()
          @fields = @label
          @label = null
      
      renderLi : (model) ->
        ul = $("<ul class='form-fieldset'></ul>")
        
        for key, field of @fields
          do (key)->
            valChanger = (newValue) =>
              model.set key, newValue
            
            ul.append field.renderLi model.get(key), valChanger
        
        wrap = $("<li></li>")
        if @label
          wrap.append "<h3>#{htmlEscape(@label)}</h3>"
        wrap.append ul
        wrap
        
      validates : () ->
        for k, field of @fields
          if not field.validates() then return false
        return true
        
    #
    # FORM ERRORS
    #
    
    exports.ValueException = class ValueException extends Error
      
      constructor : (@errorMessage) ->
        super(@errorMessage)
        
    #
    # FIELDS
    #
    
    exports.Field = class Field
      
      LI_TEMPLATE : """
        <li>
          {label}
          {tooltip}
          <div class='form-error' style='display:none;'></div>
          {input} 
        </li>"""
        
      errors : []
      
      constructor : (@label, opts={}) -> 
        @tooltip = opts.tooltip or ""
      
      renderLi : (value, triggerValueChange, onValueChange, opts={}) ->
        @renderWithTemplate @LI_TEMPLATE, value, triggerValueChange
      
      renderWithTemplate : (tpl, value, triggerValueChange, onValueChange, opts) ->
        
        tooltipHtml = if @tooltip.length > 0 then "<div class='form-tooltip'><a><span class='form-tooltip-icon'></span><span class='form-tooltip-text'>#{@tooltip}</span></a></div>" else ""
        r = $ Nano.compile tpl, {
          label : "<label class='form-label'>#{@label}</label>"
          input : "<div class='__PLACEHOLDER__'></div>"
          tooltip : tooltipHtml
        }
        
        wrappedTriggerValueChange = (val) =>
          try
            @hideError()
            @setErrors []
            triggerValueChange @cleanValue val
          catch e
            @setErrors [e.errorMessage]
            @showError r, e.errorMessage
        
        $('.__PLACEHOLDER__', r).replaceWith(@createElement(value, wrappedTriggerValueChange))
        r
      
      ### Called with a value from the UI, meant to make
      sure the value is ready to be inserted into the model.
      
      Override this to add UI validation code. If the value is
      not to your liking, throw a ValueException with a description
      of why the value is incorrect.
      ###
      cleanValue : (value) -> value
      
      setErrors : (@errors) ->
      
      validates : () -> @errors.length == 0 
      
      hideError : (element) ->
        $('.form-error', element).hide()
      
      showError : (element, errorMessage) ->
        errorEl = $('.form-error', element)
        errorEl.html(errorMessage)
        errorEl.show()
    
    
    exports.TextField = class TextField extends Field

      createElement : (value, triggerValueChange) =>
        el = $ "<input type='text' value='#{htmlEscape(value)}' />"
        el.change () -> triggerValueChange(el.val())
        el
    
    
    exports.NumberField = class NumberField extends TextField

      cleanValue : (value) -> 
        value = Number(value)
        if !_(value).isNumber()
          throw new ValueException("Value must be a number")
        value
    
    
    exports.ColorField = class ColorField extends Field
      
      createElement : (value, triggerValueChange) =>
        el = $ "<div class='colorpicker-input' style='background-color: #{htmlEscape(value)}'></div>"
        el.ColorPicker
          onChange: (hsb, hex, rgb) ->
            el.css 'background-color' : "##{hex}"
          onBeforeShow: () ->
            color = new RGBColor(el.css 'background-color')
            el.ColorPickerSetColor color.toHex()
          onHide: (hsb, hex, rgb) ->
            color = new RGBColor(el.css 'background-color')
            triggerValueChange color.toRGB()
        el
        
    return exports
          
)
