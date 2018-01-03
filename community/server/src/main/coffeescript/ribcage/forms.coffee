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
  ['ribcage/View'
   'ribcage/security/HtmlEscaper'
   'ribcage/ui/Nano'
   'ribcage/ui/Dialog'
   'ribcage/ui/ImagePicker'
   'lib/colorpicker'
   'lib/rgbcolor'],
  (View, HtmlEscaper,Nano,Dialog,ImagePicker) ->
    exports = {}
    
    
    exports.ModelForm = class ModelForm extends View

      createFields : () -> {}

      initialize : (opts)->
        @fields = @createFields()
        
        for key, field of @fields
          field.setPropertyKey key
          field.setModel @model
        
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
        
      setModel : (model) =>
        for key, field of @fields
          field.setPropertyKey key
          field.setModel model
          
      setPropertyKey : (propertyKey) =>
      
      renderLi : (model) ->
        ul = $("<ul class='form-fieldset'></ul>")
        
        for key, field of @fields
          ul.append field.renderLi()
        
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
      
      constructor : (@label, @opts={}) -> 
        @tooltip = @opts.tooltip or ""
        
      setModel : (@model) =>
      setPropertyKey : (@propertyKey) =>
      
      renderLi : () ->
        @renderWithTemplate @LI_TEMPLATE
      
      renderWithTemplate : (tpl) ->
        
        tooltipHtml = if @tooltip.length > 0 then "<div class='form-tooltip'><a><span class='form-tooltip-icon'></span><span class='form-tooltip-text'>#{@tooltip}</span></a></div>" else ""
        @el = $ Nano.compile tpl, {
          label : "<label class='form-label'>#{@label}</label>"
          input : "<div class='__PLACEHOLDER__'></div>"
          tooltip : tooltipHtml
        }
        
        @widget = @renderWidget()
        $('.__PLACEHOLDER__', @el).replaceWith(@widget)
        
        @updateUIValue()
        @el
        
      ### Update the UI value to match that
      of the underlying model.
      ###
      updateUIValue : () ->
        @setUIValue @model.get @propertyKey
      
      ### Set the value to be displayed in the UI
      ###
      setUIValue : (val)->
        @widget.val val
     
      ### Set the value in the model. Fields are expected
      to call this method (or an overridden version) whenever
      the user changes the value in the UI.
      ### 
      setModelValue : (val) ->
        try
          @setErrors []
          val = @cleanUIValue val
          @model.set @propertyKey, val
        catch e
          @setErrors [e.errorMessage]
      
      ### Called with a value from the UI, meant to make
      sure the value is ready to be inserted into the model.
      
      Override this to add UI validation code. If the value is
      not to your liking, throw a ValueException with a description
      of why the value is incorrect.
      ###
      cleanUIValue : (value) -> value
      
      ### Set a list of errors that currently applies to this field.
      Used by the #validates() method.
      ### 
      setErrors : (@errors) ->
        if @errors.length == 0 then @hideErrorBox()
        else @showErrorBox @errors[0] # TODO: Show all errors
      
      validates : () -> @errors.length == 0 
      
      hideErrorBox : () ->
        $('.form-error', @el).hide()
      
      showErrorBox : (errorMessage) ->
        errorEl = $('.form-error', @el)
        errorEl.html(errorMessage)
        errorEl.show()
    
    
    exports.TextField = class TextField extends Field

      renderWidget : () =>
        el = $ "<input type='text' class='form-input' value='' />"
        el.change () => @setModelValue el.val()
        el
    
    
    exports.NumberField = class NumberField extends TextField

      cleanUIValue : (value) -> 
        value = Number(value)
        if !_(value).isNumber()
          throw new ValueException("Value must be a number")
        value
        
    exports.ImageURLField = class ImageURLField extends Field

      renderWidget : () =>
        @urlInput = $ "<input type='text' class='form-input' value='' />"
        @urlInput.change () => 
          @setModelValue @urlInput.val()
          @updateImageElementUrl @urlInput.val()
        
        @imageElement = $ "<img class='form-image-url-field-preview'/>"
        
        wrap = $ "<div class='form-image-url-field'></div>"
        metaBar = $ "<div class='form-image-url-field-metabar'></div>"
        metaBar.append "<span class='form-image-url-field-preview-title small'>Preview:</span>"
        
        wrap.append @urlInput
        wrap.append metaBar
        
        if @opts['imageUrls']?
        
          @imagePicker ?= new ImagePicker(@opts['imageUrls'])
          $(@imagePicker.el).addClass "imagepicker-dialog"
          @dialog ?= new Dialog @imagePicker
          @dialog.el.prepend "<h1>Pick an image you like</h1>"
          
          @imagePicker.bind 'image:clicked', (ev) =>
            @setUIValue ev.url
            @setModelValue ev.url
            @dialog.hide()
            
          pickerButton = $ "<a href='#' class='form-image-url-field-builtin micro-button'>Built in images</a>"
          pickerButton.click (ev) => 
            ev.preventDefault()
            @dialog.show()
          metaBar.append pickerButton
          
        wrap.append @imageElement
        
        wrap
      
      setUIValue : (value) =>
        @urlInput.val value
        @updateImageElementUrl value
      
      updateImageElementUrl  : (value) =>
        @imageElement.attr('src', value)

    
    exports.ColorField = class ColorField extends Field
      
      renderWidget : () =>
        el = $ "<div class='colorpicker-input'></div>"
        el.ColorPicker
          onChange: (hsb, hex, rgb) ->
            el.css 'background-color' : "##{hex}"
          onBeforeShow: () ->
            color = new RGBColor(el.css 'background-color')
            el.ColorPickerSetColor color.toHex()
          onHide: (hsb, hex, rgb) =>
            color = new RGBColor(el.css 'background-color')
            @setModelValue color.toRGB()
        el
        
      setUIValue : (color)=>
        @widget.css('background-color', color)
    
    
    exports.FormChooserField = class FormChooserField extends Field
      
      constructor : (@label, @options) ->
        super(@label)
      
      renderWidget : () =>
        @select = $ "<select></select>"
        for key, option of @options
          @select.append "<option value='#{htmlEscape(key)}'>#{htmlEscape(option.label)}</option>"
        @select.change (ev) => 
          @setModelValue $(ev.target).val()
          @showForm $(ev.target).val()
        
        wrapper = $ "<div class='form-chooser-field'></div>"
        @formContainer = $ "<div class='form-chooser-form-container'></div>"
        
        wrapper.append @select
        wrapper.append @formContainer
        
        wrapper
      
      setUIValue : (formKey) =>
        @select.val(formKey)
        @showForm formKey
        
      showForm : (formKey) =>
        if not @options[formKey]?
          for key, opt of @options
            formKey = key
            break
        
        @formContainer.html("")
        @formContainer.append @options[formKey].form.render().el
    
    exports.FormChooserOption = class FormChooserOption
        constructor : (@label, @form) ->
        
    return exports
          
)
