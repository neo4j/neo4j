window.neo = {}

neo.models = {}

neo.renderers =
  node: []
  relationship: []

neo.utils =
  # Note: quick n' dirty. Only works for serializable objects
  copy: (src) ->
    JSON.parse(JSON.stringify(src))

  extend: (dest, src) ->
    return if not neo.utils.isObject(dest) and neo.utils.isObject(src)
    dest[k] = v for own k, v of src
    return dest

  isArray: Array.isArray or (obj) ->
    Object::toString.call(obj) == '[object Array]';

  isObject: (obj) -> Object(obj) is obj
