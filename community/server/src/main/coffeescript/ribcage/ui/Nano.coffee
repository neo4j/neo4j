
define [], () ->
  # Nano Templates (Tomasz Mazur, Jacek Becela)
  class Nano
    
    @compile : (template, data) ->
      template.replace /\{([\w\-\.]*)}/g, (str, key) ->
        keys = key.split(".")
        value = data[keys.shift()]
        for key in keys
          if value.hasOwnProperty(key) then value = value[key] 
          else value = str
        value

