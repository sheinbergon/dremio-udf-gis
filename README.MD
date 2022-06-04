[![GitHub](https://img.shields.io/github/license/sheinbergon/dremio-udf-gis?color=pink&style=for-the-badge)](https://github.com/sheinbergon/dremio-udf-gis/blob/master/LICENSE)
[![Github Workflow Status](https://img.shields.io/github/workflow/status/sheinbergon/dremio-udf-gis/Release%20Actions?style=for-the-badge)](https://github.com/sheinbergon/dremio-udf-gis/actions?query=workflow%3Arelease-actions)
[![GitHub release (latest by date)](https://img.shields.io/github/v/release/sheinbergon/dremio-udf-gis?color=%2340E0D0&style=for-the-badge)](https://github.com/sheinbergon/dremio-udf-gis/releases/latest)
[![Maven Central](https://img.shields.io/maven-central/v/org.sheinbergon/dremio-udf-gis?color=Crimson&style=for-the-badge)](https://search.maven.org/search?q=g:org.sheinbergon%20a:dremio-udf-gis*)
[![Snyk Vulnerabilities for GitHub Repo](https://img.shields.io/snyk/vulnerabilities/github/sheinbergon/dremio-udf-gis?color=432f95&style=for-the-badge)](https://app.snyk.io/org/sheinbergon/project/94183993-505b-439c-9078-6276fa4c1626)


# Dremio GIS

### Adds GIS functionality to Dremio.
Derived from:   
- [Apache Drill GIS Functionality](https://github.com/apache/drill/tree/master/contrib/udfs/src/main/java/org/apache/drill/exec/udfs/gis)  
- [Christy Haragan's initial port](https://github.com/christyharagan/dremio-gis)  

### What you get
- Widespread OGC implementation for SQL (adheres to PostGIS standards)
  - Supported input formats: `WKT`, `WKB (HEX or BINARY)`
  - Supported output formats: `WKT`, `WKB`, `GeoJSON` 
- Easily installable Maven-Central/Github artifacts shaded jar artifact  
- Dremio CE version compatibility (new versions will be released with each community edition) 
- Up-2-date Proj4J & JTS geometry based implementation

### Installation
- Take the shaded jar for the desired version and place inside your Dremio installation (`$DREMIO_HOME/jars/3rdparty`)
- Restart your Dremio server(s)
- Rejoice! (and see the [Cheat Sheet](https://github.com/sheinbergon/dremio-udf-gis/wiki/GIS-Functions-Cheat-Sheet) for detailed usage instructions)

#### Version Compatbility

| Library Version | Dremio Version |
|-----------------|----------------|
| 0.2.x           | 20.1.x         |
| 0.3.x           | 21.1.x         |
| 0.4.x           | 21.2.x         |

### Usage Notes
As opposed to PostGIS, Dremio is only a query engine based on existing/projected data sources/lakes.  
That means that `Geometry` is not a natively supported data type, and you can only access it if     
it's being properly projected from the data sources (For example, PostGIS Geometry is read as an `EWKB` HEX encoded string).

In order to successfully use the provided GIS functions, you must first make sure the geometry is in `WKB (BINARY)` format.
If it's not, you need to decode it:  
- if the input is in `WKT` format, use `ST_GeomFromText`
- if the input is a HEX encoded`WKB`, use Dremio's `FROM_HEX`

This library uses Dremios' Arrow buffers (`ArrowBuf`) to maintain geometry data in binary (`WKB`) format (for performance and efficiency)  
when interchanging it between GIS functions, which is of course undecipherable for the naked eye. When running queries from the UI,  
`WKB` output will always be base64 encoded. 

In order to resolve Data back to human-readable format (`WKT`), use `ST_AsText`/`ST_AsGeoJson`

Example:

```sql
SELECT ST_AsText(
    ST_Difference(
      ST_GeomFromText('LINESTRING(50 100, 50 200)'),
        ST_GeomFromText('LINESTRING(50 50, 50 150)')
))
```

### Roadmap
- Frequent version/dependency updates
- Add more OGC/PostGIS matching functionality
