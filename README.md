# Graph Validator

Library and command line tools for validating that a graph conforms to the Schema.org format,
optionally including additional schemas in the same format.

To validate a graph, use the `ValidateGraph` tool:

    java -jar validator-1.0.jar uk.gov.nca.graph.validator.cli.ValidateGraph -g graphml.properties

To validate a mapping file (see the `graph-mapper` project), use the `ValidateMap` tool,
which generates a sample graph from the mapping and then validates that:

    java -jar validator-1.0.jar uk.gov.nca.graph.validator.cli.ValidateMap -g graphml.properties

The following rules are applied when validating the graph:

* Properties whose expected types are one of the defined Schema.org data types (i.e. derive from DataType) should be represented as properties on the object
* Properties whose expected types are another object (i.e. derive from Thing) should be represented as relationships
* Objects deriving from the Action type should be represented as objects, not as relationships
