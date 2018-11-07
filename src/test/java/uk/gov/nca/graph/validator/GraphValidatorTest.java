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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.LocalDate;
import java.time.Month;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;
import uk.gov.nca.graph.utils.GraphUtils;

public class GraphValidatorTest {
  @Test
  public void testValidatorProperties() throws Exception {
    try (GraphValidator gv = new GraphValidator(true)) {
      Graph graph = TinkerGraph.open();

      //Create a Schema.org valid graph
      Vertex v1 = graph.addVertex(T.label, "Person",
          "name", "Abraham Lincoln",
          "birthDate", LocalDate.of(1809, Month.FEBRUARY, 12));
      Vertex v2 = graph.addVertex(T.label, "Place", "name", "Hodgenville, Kentucky");

      v1.addEdge("birthPlace", v2);

      assertTrue(gv.validate(graph));

      //Modify graph to be invalid
      v1.property("favouritePet", "Cat");

      assertFalse(gv.validate(graph));

      GraphUtils.closeGraph(graph);
    }
  }

  @Test
  public void testValidatorBadNode() throws Exception {
    try (GraphValidator gv = new GraphValidator(true)) {
      Graph graph = TinkerGraph.open();

      graph.addVertex("Animal");
      assertFalse(gv.validate(graph));

      GraphUtils.closeGraph(graph);
    }
  }

  @Test
  public void testValidatorBadLink() throws Exception {
    try (GraphValidator gv = new GraphValidator(true)) {
      Graph graph = TinkerGraph.open();

      //Create a Schema.org valid graph
      Vertex v1 = graph.addVertex(T.label, "Person");
      Vertex v2 = graph.addVertex(T.label, "Organization");

      //Owns can't link to an Organization, only to a Product
      v1.addEdge("owns", v2);
      assertFalse(gv.validate(graph));

      GraphUtils.closeGraph(graph);
    }
  }

  @Test
  public void testValidatorLinkOnParent() throws Exception {
    try (GraphValidator gv = new GraphValidator(true)) {
      Graph graph = TinkerGraph.open();

      //Create a Schema.org valid graph
      Vertex v1 = graph.addVertex(T.label, "DiscussionForumPosting"); //DiscussionForumPosting is a sub-class of CreativeWork
      Vertex v2 = graph.addVertex(T.label, "Collection");   //Collection is a sub-class of CreativeWork, defined in Bib

      v1.addEdge("isPartOf", v2); //isPartOf is on CreativeWork, with a target to CreativeWork
      assertTrue(gv.validate(graph));

      GraphUtils.closeGraph(graph);
    }
  }
}
