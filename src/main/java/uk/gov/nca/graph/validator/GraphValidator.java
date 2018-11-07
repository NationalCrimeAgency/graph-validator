/*
National Crime Agency (c) Crown Copyright 2018

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package uk.gov.nca.graph.validator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.utils.ElementUtils;

/**
 * Validator class which creates an in-memory graph of the Schema.org schema
 * (including extensions, and any additional schema files passed) and uses
 * that graph to validate the structure of data graphs.
 */
public class GraphValidator implements AutoCloseable{
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphValidator.class);
  private static final Pattern PATTERN_URL = Pattern.compile("^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", Pattern.CASE_INSENSITIVE);

  private static final String PARENT = "_parent";

  private static final String TYPE_BOOLEAN = "Boolean";
  private static final String TYPE_DATE = "Date";
  private static final String TYPE_DATETIME = "DateTime";
  private static final String TYPE_NUMBER = "Number";
  private static final String TYPE_FLOAT = "Float";
  private static final String TYPE_INTEGER = "Integer";
  private static final String TYPE_TEXT = "Text";
  private static final String TYPE_URL = "URL";
  private static final String TYPE_TIME = "Time";

  private ObjectMapper mapper = new ObjectMapper();
  private Graph schemaGraph = TinkerGraph.open();

  /**
   * Create a new instance of the graph validator, with configuration
   */
  public GraphValidator(boolean includeSuperseded, InputStream... additionalStreams) {
    List<Map<String, Object>> schemaList = new ArrayList<>();

    //Load built-in schemas
    loadStream(schemaList, GraphValidator.class.getResourceAsStream("schema.jsonld"), "Schema.org");
    loadStream(schemaList, GraphValidator.class.getResourceAsStream("ext-auto.jsonld"), "Auto");
    loadStream(schemaList, GraphValidator.class.getResourceAsStream("ext-bib.jsonld"), "Bib");
    loadStream(schemaList, GraphValidator.class.getResourceAsStream("ext-health-lifesci.jsonld"), "Health/Life Science");
    loadStream(schemaList, GraphValidator.class.getResourceAsStream("ext-meta.jsonld"), "Meta");
    loadStream(schemaList, GraphValidator.class.getResourceAsStream("ext-pending.jsonld"), "Pending");

    //Load additional schemas
    for(int i = 0; i < additionalStreams.length; i++)
      loadStream(schemaList, additionalStreams[i], "Additional Stream #"+i);

    //Parse all schemas into our schema graph
    loadSchemas(schemaList, includeSuperseded);
  }

  @SuppressWarnings("unchecked")    //ClassCastException is caught, as we can't check List<Map<String, Object>>
  private void loadStream(List<Map<String, Object>> schemaList, InputStream inputStream, String name){
    LOGGER.info("Loading schema {}", name);

    try {
      //Read schema (assumed to be JSON) and add to the list of all schema objects
      Map<String, Object> schema = mapper
        .readValue(inputStream, new TypeReference<Map<String, Object>>() {
      });

      schemaList.addAll((List<Map<String, Object>>) schema.get("@graph"));
    }catch (IOException ioe){
      LOGGER.error("Unable to read schema {} from Stream", name, ioe);
    }catch (ClassCastException cce){
      LOGGER.error("Unable to read @graph object from schema {} - not in expected format", name, cce);
    }
  }

  private void loadSchemas(List<Map<String, Object>> schema, boolean includeSuperseded){
    Map<String, Property> properties = new HashMap<>();

    //Loop through all items in the schema
    schema.forEach(m -> {
      //If we're not including superseded classes, then don't process them
      if (!includeSuperseded && m.containsKey("http://schema.org/supersededBy"))
        return;

      //Get @id of object
      Object objId = m.get("@id");
      if(objId == null){
        LOGGER.warn("Schema item with no @id found - item will be skipped");
        return;
      }

      //Store commonly used values
      String id = objId.toString();
      String label = getLabel(m);

      List<String> types = getAsStringList(m.get("@type"));

      if(types.contains("rdf:Property")){   //If this object is a property, then merge it with other properties for later
        if(label == null) {
          LOGGER.warn("Unable to determine label for property");
          return;
        }

        Property prop = properties.getOrDefault(id, new Property(label));

        prop.addDomain(getAsIdList(m.get("http://schema.org/domainIncludes")));
        prop.addRange(getAsIdList(m.get("http://schema.org/rangeIncludes")));

        properties.put(id, prop);
      }else{    //If not a property, then add it to our graph
        Vertex v = addOrCreateVertex(schemaGraph, id);

        //Add label
        if(label != null)
          v.property("l", label);

        //Add parent information
        List<String> parentIds = getAsIdList(m.get("rdfs:subClassOf"));
        for(String parentId : parentIds){
          addOrCreateVertex(schemaGraph, parentId).addEdge(PARENT, v);
        }
      }
    });

    //Add property links
    for(Entry<String, Property> property : properties.entrySet()){
      for(String domain : property.getValue().getDomain()){
        for(String range : property.getValue().getRange()){
          Vertex d = addOrCreateVertex(schemaGraph, domain);
          Vertex r = addOrCreateVertex(schemaGraph, range);

          d.addEdge(property.getValue().getLabel(), r);
        }
      }
    }
  }

  /**
   * Validate the structure of graph against our schema graph.
   * Returns true if the graph is valid, and false otherwise.
   */
  public boolean validate(Graph graph){
    //Loop through every vertex
    Iterator<Vertex> iterV = graph.vertices();
    while(iterV.hasNext()) {
      Vertex v = iterV.next();

      //Return as soon as we find an invalid node, rather than continuing
      if(!isValid(v))
        return false;
    }

    return true;
  }

  private boolean isValid(Vertex v){
    String vertexLabel = v.label();

    //Check that the vertex has a valid name (i.e. a node with the same label exists in the property graph)
    List<Vertex> schemaVertices = schemaGraph.traversal().V().has("l", vertexLabel).toList();

    if(schemaVertices.isEmpty()) {
      LOGGER.info("Could not find vertex {} in the schema", vertexLabel);
      return false;   //Nothing in graph with this label
    }

    //This shouldn't happen (to classes with the same label), but warn if it does
    if(schemaVertices.size() > 1)
      LOGGER.warn("More than one class in schema found with the label {}; only the first one will be used", vertexLabel);

    Vertex schemaVertex = schemaVertices.get(0);

    //Check the properties on the vertex
    Iterator<VertexProperty<Object>> iterProp = v.properties();
    while(iterProp.hasNext()){
      VertexProperty<Object> vp = iterProp.next();
      if(!vp.isPresent())
        continue;

      String propertyLabel = vp.label();
      String propertyValueClass = valueToDataType(vp.value());

      if(!hasProperty(schemaVertex, propertyLabel, propertyValueClass)) {
        LOGGER.info("Could not find property {} on vertex {} with a target of {} in the schema", propertyLabel, vertexLabel, propertyValueClass);
        return false;
      }
    }

    //Check the edges OUT from the vertex (don't worry about IN), they will be checked by on other vertices
    Iterator<Edge> iterEdges = v.edges(Direction.OUT);
    while(iterEdges.hasNext()){
      Edge e = iterEdges.next();

      String edgeLabel = e.label();
      String edgeTargetClass = e.inVertex().label();

      if(!hasProperty(schemaVertex, edgeLabel, edgeTargetClass)) {
        LOGGER.info("Could not find edge {} on vertex {} with a target of {} in the schema", edgeLabel, vertexLabel, edgeTargetClass);
        return false;
      }
    }

    //If we've got here, then we must be valid
    return true;
  }

  private boolean hasProperty(Vertex v, String property, String dataType){
    //Find a list of all target vertices in the schema for the given property
    List<Vertex> vertices = schemaGraph.traversal().V(v.id()).out(property).toList();

    //Check that the target type matches, recursive as it might be a sub-type
    for(Vertex targetV : vertices){
      if(ofType(targetV, dataType))
        return true;
    }

    //If the property didn't exist on this node, check any parents
    Iterator<Edge> iterEdges = v.edges(Direction.IN, PARENT);
    while(iterEdges.hasNext()){
      Edge e = iterEdges.next();

      if(hasProperty(e.outVertex(), property, dataType))
        return true;
    }

    //If we haven't returned true by now, then the property doesn't exist
    return false;
  }

  private boolean ofType(Vertex v, String type){
    //If the type matches, then return true
    if(type.equals(ElementUtils.getProperty(v, "l")))
      return true;

    //If it doesn't, then check the children (not the parents) to see if it is a valid subtype
    Iterator<Edge> iterEdges = v.edges(Direction.OUT, PARENT);
    while(iterEdges.hasNext()){
      Edge e = iterEdges.next();

      if(ofType(e.inVertex(), type))
        return true;
    }

    //If we haven't returned true by now, then the type is incorrect
    return false;
  }

  private static List<Object> getAsList(Object o){
    //Return an empty list of o is null
    if(o == null)
      return Collections.emptyList();

    if(o instanceof List){    //If o is already a list, return the list
      return (List<Object>) o;
    }else{    //Otherwise create a single valued list
      return Arrays.asList(o);
    }
  }

  private String getLabel(Map<String, Object> m) {
    if (m.containsKey("@label")){      //Get the label from the map directly if it exists
      return m.get("@label").toString();
    }else if(m.containsKey("@id")) {   //If the label doesn't exist, extract a label from the ID
      String[] parts = m.get("@id").toString().split("/");
      return parts[parts.length - 1];
    }else{    //If the ID doesn't exist, return null
      return null;
    }
  }

  private static List<String> getAsStringList(Object o){
    //Get the object as a list, and map everything to a String
    return getAsList(o).stream().map(Object::toString).collect(Collectors.toList());
  }

  private static List<String> getAsIdList(Object o){
    //Return a list of IDs, extracted from one or more maps
    return getAsList(o).stream()
      .filter(obj -> Map.class.isAssignableFrom(obj.getClass()))
      .map(obj -> ((Map<Object, Object>)obj).get("@id"))
      .filter(Objects::nonNull)
      .map(Object::toString)
      .collect(Collectors.toList());
  }

  private static Vertex addOrCreateVertex(Graph graph,  String id){
    //Get existing vertices with this ID
    List<Vertex> vertices = graph.traversal().V(id).toList();

    if(vertices.isEmpty()){   //If no vertices with that ID exist, create one
      return graph.addVertex(T.id, id);
    }else{
      return vertices.get(0); //Otherwise return the first item in the list (should only be one)
    }
  }

  private static String valueToDataType(Object o) {
    Class c = o.getClass();

    if (c == Boolean.class) {
      return TYPE_BOOLEAN;
    }else if (c == LocalDate.class) {
      return TYPE_DATE;
    }else if (c == LocalDateTime.class || c == ZonedDateTime.class) {
      return TYPE_DATETIME;
    }else if (c == Float.class || c == Double.class) {
      return TYPE_FLOAT;
    }else if (c == Integer.class || c == Long.class) {
      return TYPE_INTEGER;
    }else if (Number.class.isAssignableFrom(c)){
      return TYPE_NUMBER;
    }else if (c == String.class){
      String s = (String) o;
      //Is it a date or a time?

      try{
        LocalTime.parse(s, DateTimeFormatter.ISO_TIME);
        return TYPE_TIME;
      }catch (DateTimeParseException pe){
        //Do nothing, expected if this isn't a time
      }

      try{
        LocalTime.parse(s, DateTimeFormatter.ISO_DATE);
        return TYPE_DATE;
      }catch (DateTimeParseException pe){
        //Do nothing, expected if this isn't a date
      }

      try{
        LocalTime.parse(s, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return TYPE_DATETIME;
      }catch (DateTimeParseException pe){
        //Do nothing, expected if this isn't a date time
      }

      try{
        LocalTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        return TYPE_DATETIME;
      }catch (DateTimeParseException pe){
        //Do nothing, expected if this isn't a date time
      }

      if(PATTERN_URL.matcher(s).matches())
        return TYPE_URL;

      return TYPE_TEXT;
    }else if (c == URL.class){
      return TYPE_URL;
    }else if (c == LocalTime.class){
      return TYPE_TIME;
    }

    return "UNKNOWN";
  }

  @Override
  public void close() throws Exception {
    schemaGraph.close();
  }

  /**
   * Class to hold domains and ranges of a property
   */
  private class Property {
    private final String label;
    private final Set<String> domain = new HashSet<>();
    private final Set<String> range = new HashSet<>();

    public Property(String label){
      this.label = label;
    }

    private void addDomain(String domain){
      this.domain.add(domain);
    }

    private void addDomain(Collection<String> domain){
      domain.forEach(this::addDomain);
    }

    private Set<String> getDomain(){
      return domain;
    }

    private void addRange(String range){
      this.range.add(range);
    }

    private void addRange(Collection<String> range){
      range.forEach(this::addRange);
    }

    private Set<String> getRange(){
      return range;
    }

    public String getLabel() {
      return label;
    }
  }
}
