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

package uk.gov.nca.graph.validator.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.nca.graph.validator.GraphValidator;

/**
 * Main entry point for application, which validates a Graph against Schema.org
 */
public class ValidateGraph {
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidateGraph.class);

  public static void main(String[] args){
    //Configure command line parameters and parse
    Options options = new Options();

    Option optG = new Option("g", "graph", true, "Tinkerpop graph configuration file");
    optG.setRequired(true);
    options.addOption(optG);

    options.addOption("s", "schemas", true, "Additional schema files");
    options.addOption("d", "deprecated", false, "Accept deprecated types in schema");

    CommandLineParser clParser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = clParser.parse(options, args);

    } catch (ParseException e) {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("java -cp cli-1.0.jar uk.gov.nca.graph.validator.cli.ValidateGraph", options);
      return;
    }

    String[] schemas = cmd.getOptionValues('s');
    List<InputStream> streams = new ArrayList<>();

    if(schemas != null) {
      LOGGER.info("Opening InputStreams to additional schemas");
      //Load additional data sources
      for (String schema : schemas) {
        try {
          streams.add(new FileInputStream(schema));
        } catch (IOException ioe) {
          LOGGER.warn("Unable to open stream to schema", ioe);
        }
      }
    }

    String[] graphs = cmd.getOptionValues('g');

    //Create validator
    LOGGER.info("Initialising validator");
    try (
      GraphValidator validator = new GraphValidator(cmd.hasOption('d'), streams.toArray(new InputStream[streams.size()]))
    ){
      for(String graph : graphs) {
        validateGraph(validator, graph);
      }
    }catch (Exception e){
      LOGGER.error("Exception occurred whilst closing validator", e);
    }

    //Close InputStreams
    if(!streams.isEmpty()) {
      LOGGER.info("Closing InputStreams");
      streams.forEach(is -> {
        try {
          is.close();
        } catch (IOException ioe) {
          LOGGER.warn("Exception closing InputStream", ioe);
        }
      });
    }

    LOGGER.info("Finished");
  }

  private static void validateGraph(GraphValidator validator, String graphConfigFile){
    try(
        Graph g = GraphFactory.open(graphConfigFile)
    ) {
      LOGGER.info("Validating graph specified by {}", graphConfigFile);
      if(validator.validate(g)){
        LOGGER.info("Graph {} was successfully validated", graphConfigFile);
      }else{
        LOGGER.warn("Graph {} was not valid", graphConfigFile);
      }

    }catch (Exception e){
      LOGGER.error("Unable to validate graph {}, an exception occurred", graphConfigFile, e);
    }
  }
}