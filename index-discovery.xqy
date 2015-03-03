xquery version "1.0-ml";

(:~
 : methods for discovering indices, grouped by root QNames
 :
 : @author Joe Bryan
 : @version 1.0.0
 :)
module namespace idx = "http://marklogic.com/index-discovery";

import module namespace admin = "http://marklogic.com/xdmp/admin" at "/MarkLogic/admin.xqy";
import module namespace ctx = "http://marklogic.com/cts-extensions"
  at "/mlpm_modules/cts-extensions/cts-extensions.xqy";

declare option xdmp:mapping "false";

declare %private function idx:intersect-maps($maps as map:map*) as map:map
{
  fn:fold-left(
    function($a, $b) { $a + $b },
    map:map(),
    $maps)
};

declare %private function idx:evaluate-indexes($indexes as element()*) as map:map
{
  idx:intersect-maps(
    for $index in $indexes
    for $ref in ctx:resolve-reference-from-index($index)
    for $root in ctx:root-QNames( ctx:reference-query($ref) )
    return map:entry(xdmp:key-from-QName($root), $ref))
};

declare function idx:element-indexes() as map:map
{
  idx:element-indexes( xdmp:database() )
};

declare function idx:element-indexes($database as xs:unsignedLong) as map:map
{
  idx:evaluate-indexes(
    admin:database-get-range-element-indexes(admin:get-configuration(), $database))
};

declare function idx:element-attribute-indexes() as map:map
{
  idx:element-attribute-indexes( xdmp:database() )
};

declare function idx:element-attribute-indexes($database as xs:unsignedLong) as map:map
{
  idx:evaluate-indexes(
    admin:database-get-range-element-attribute-indexes(admin:get-configuration(), $database))
};

declare function idx:path-indexes() as map:map
{
  idx:path-indexes( xdmp:database() )
};

declare function idx:path-indexes($database as xs:unsignedLong) as map:map
{
  idx:evaluate-indexes(
    admin:database-get-range-path-indexes(admin:get-configuration(), $database))
};

declare function idx:range-indexes() as map:map
{
  idx:range-indexes( xdmp:database() )
};

declare function idx:range-indexes($database as xs:unsignedLong) as map:map
{
  idx:intersect-maps((
    idx:element-indexes($database),
    idx:element-attribute-indexes($database),
    idx:path-indexes($database)
    (: TODO: implement field, geospatial, etc. :)
  ))
};

declare function idx:all() as map:map
{
  idx:expand-references( idx:range-indexes() )
};

declare function idx:expand-references($indexes as map:map) as map:map
{
  map:new(
    for $key in map:keys($indexes)
    return
      map:entry($key,
        for $val in map:get($indexes, $key)
        return idx:reference-to-map($val)))
};

declare function idx:reference-to-map($ref) as map:map
{
  let $ref :=
    if ($ref instance of element())
    then $ref
    else document { $ref }/*
  return
    map:new((
      map:entry("ref-type", fn:local-name($ref)),
      for $x in $ref/*
      return map:entry(fn:local-name($x), $x/fn:string())))
};

declare function idx:reference-from-map($map as map:map) as cts:reference?
{
  let $ref-type := map:get($map, "ref-type")
  return
    if (fn:exists($ref-type))
    then
      cts:reference-parse(
        element { "cts:" || $ref-type } {
          for $key in map:keys($map)[. ne "ref-type"]
          return
            element { "cts:" || $key } {
              map:get($map, $key)
            }
        })
    else ()
};
