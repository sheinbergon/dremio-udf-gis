# Dremio GIS

### Adds GIS functionality to Dremio. Derived from:   
- [Apache Drill GIS Functionality](https://github.com/apache/drill/tree/master/contrib/udfs/src/main/java/org/apache/drill/exec/udfs/gis)  
- [Christy Haragan's initial port](https://github.com/christyharagan/dremio-gis)  

### What you get
- Widespread OGC implementation for SQL (adheres to PostGIS standards)
  - Supported input formats: `WKT`
  - Supported output formats: `WKT`, `JSON`, `GeoJSON` 
- Easily installable Maven-Central/Github artifacts shaded jar artifact  
- Dremio CE version compatibility (new versions will be released with each community edition) 
- Up-2-date Proj4J & ESRI geometry based implementation

### Installation
- Take the shaded jar for the desired version and place inside your Dremio installation (`$DREMIO_HOME/jars/3rdparty`)
- Restart your Dremio server(s)
- Rejoice!

### Usage Notes
As opposed to PostGIS, Dremio is only a query engine based on existing/projected data sources/lakes.
That means that:
- `Geometry` is not a supported data type, meaning you can't query it directly
- Metadata information, such as `SRID`, cannot be tied to the geometry data.  
  To be more specific, this implementation is tied to the ESRI geometry API serialization capabilities.

In order to successfully use the provided functionality, you must first decode data to an `OGCGeometry` type, using `ST_GeomFromText`.  
This library uses Dremios' Arrow buffers (`ArrowBuf`) to maintain geometry data in binary (`WKB`) format (for performance and efficiency) when  
interchanging it between GIS functions, which is of course undecipherable for the naked eye.  

In order to resolve Data back to human readable format (`WKT`), use `ST_AsText`/`ST_AsGeoJson`

Example:

```roomsql
SELECT ST_AsText(
    ST_Difference(
     ST_GeomFromText('LINESTRING(50 100, 50 200)'),
        ST_GeomFromText('LINESTRING(50 50, 50 150)')
))
```

### Roadmap
- Frequent version/dependency updates
- Serialize `SpatialReference` alongside WKB to preserve SRID information
- Add more OGC/PostGIS matching functionality

### Caveats
 - Due to SRID not being serialized when calling [OGCGeometry.asBinary()](https://esri.github.io/geometry-api-java/javadoc/com/esri/core/geometry/ogc/OGCGeometry.html#asBinary--),
   calling `ST_GeomFromText(<WKT>,<SRID>)` will not effect the SRID propelled to the GIS functions down the stream, rendering SRID parametrization useless.