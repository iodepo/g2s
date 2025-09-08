# Product Flow



```mermaid
flowchart TD
    ODIS[ODIS Graph] --> A
    SDO[Schema.org] --> Ar
    SDO --> E
    

    A(Query Process\n For example on type Dataset) --> B(Lance Table)
    C --> B
    B --> C[Augment]
    
    
    
    B --> Ar[Arrow: Memory Table]
    Ar --> E[Export Functions]

    E --> K[JSON Lines]
    E --> P[Parquet]
    E --> G[GeoPackage \n GeoParquet]
    E --> CSV[CSV]
    E --> O[Other]
    
    E --> PG[Property Graph]
    
classDef golden fill:#B8860B, stroke:#DAA520
    class ODIS,A,B,C,Ar,E,K golden

```


## Notes 
The export functions generate products.  These products can be augmented with schema.org types and properties.

Also, we can generate metadata for these products, but do not currently, based on:

* ODIS Patterns
* Science on Schema.org
* CDIF Profile
* Croissant Profile

A product plus metadata could be published set if we felt we could address the citation and license elements.  

## Questions
Granularity can be of:

* type 
* provider + type.  

