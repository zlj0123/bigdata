/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.examples.java.graph;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsSecond;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.examples.java.util.DataSetDeprecationInfo.DATASET_DEPRECATION_INFO;

/**
 * An implementation of the connected components algorithm, using a delta iteration.
 *
 * <p>Initially, the algorithm assigns each vertex an unique ID. In each step, a vertex picks the
 * minimum of its own ID and its neighbors' IDs, as its new ID and tells its neighbors about its new
 * ID. After the algorithm has completed, all vertices in the same component will have the same ID.
 *
 * <p>A vertex whose component ID did not change needs not propagate its information in the next
 * step. Because of that, the algorithm is easily expressible via a delta iteration. We here model
 * the solution set as the vertices with their current component ids, and the workset as the changed
 * vertices. Because we see all vertices initially as changed, the initial workset and the initial
 * solution set are identical. Also, the delta to the solution set is consequently also the next
 * workset.<br>
 *
 * <p>Input files are plain text files and must be formatted as follows:
 *
 * <ul>
 *   <li>Vertices represented as IDs and separated by new-line characters.<br>
 *       For example <code>"1\n2\n12\n42\n63"</code> gives five vertices (1), (2), (12), (42), and
 *       (63).
 *   <li>Edges are represented as pairs for vertex IDs which are separated by space characters.
 *       Edges are separated by new-line characters.<br>
 *       For example <code>"1 2\n2 12\n1 12\n42 63"</code> gives four (undirected) edges (1)-(2),
 *       (2)-(12), (1)-(12), and (42)-(63).
 * </ul>
 *
 * <p>Usage: <code>
 * ConnectedComponents --vertices &lt;path&gt; --edges &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;
 * </code><br>
 * If no parameters are provided, the program is run with default data from {@link
 * ConnectedComponentsData} and 10 iterations.
 *
 * <p>This example shows how to use:
 *
 * <ul>
 *   <li>Delta Iterations
 *   <li>Generic-typed Functions
 * </ul>
 *
 * <p>Note: All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a future
 * Flink major version. You can still build your application in DataSet, but you should move to
 * either the DataStream and/or Table API. This class is retained for testing purposes.
 */
@SuppressWarnings("serial")
public class ConnectedComponents {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectedComponents.class);

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String... args) throws Exception {

        LOGGER.warn(DATASET_DEPRECATION_INFO);

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final int maxIterations = params.getInt("iterations", 10);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // read vertex and edge data
        DataSet<Long> vertices = getVertexDataSet(env, params);
        DataSet<Tuple2<Long, Long>> edges = getEdgeDataSet(env, params).flatMap(new UndirectEdge());

        // assign the initial components (equal to the vertex id)
        DataSet<Tuple2<Long, Long>> verticesWithInitialId =
                vertices.map(new DuplicateValue<Long>());

        // open a delta iteration
        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                verticesWithInitialId.iterateDelta(verticesWithInitialId, maxIterations, 0);

        // apply the step logic: join with the edges, select the minimum neighbor, update if the
        // component of the candidate is smaller
        DataSet<Tuple2<Long, Long>> changes =
                iteration
                        .getWorkset()
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new NeighborWithComponentIDJoin())
                        .groupBy(0)
                        .aggregate(Aggregations.MIN, 1)
                        .join(iteration.getSolutionSet())
                        .where(0)
                        .equalTo(0)
                        .with(new ComponentIdFilter());

        // close the delta iteration (delta and new workset are identical)
        DataSet<Tuple2<Long, Long>> result = iteration.closeWith(changes, changes);

        // emit result
        if (params.has("output")) {
            result.writeAsCsv(params.get("output"), "\n", " ");
            // execute program
            env.execute("Connected Components Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Function that turns a value into a 2-tuple where both fields are that value. */
    @ForwardedFields("*->f0")
    public static final class DuplicateValue<T> implements MapFunction<T, Tuple2<T, T>> {

        @Override
        public Tuple2<T, T> map(T vertex) {
            return new Tuple2<T, T>(vertex, vertex);
        }
    }

    /**
     * Undirected edges by emitting for each input edge the input edges itself and an inverted
     * version.
     */
    public static final class UndirectEdge
            implements FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
        Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

        @Override
        public void flatMap(Tuple2<Long, Long> edge, Collector<Tuple2<Long, Long>> out) {
            invertedEdge.f0 = edge.f1;
            invertedEdge.f1 = edge.f0;
            out.collect(edge);
            out.collect(invertedEdge);
        }
    }

    /**
     * UDF that joins a (Vertex-ID, Component-ID) pair that represents the current component that a
     * vertex is associated with, with a (Source-Vertex-ID, Target-VertexID) edge. The function
     * produces a (Target-vertex-ID, Component-ID) pair.
     */
    @ForwardedFieldsFirst("f1->f1")
    @ForwardedFieldsSecond("f1->f0")
    public static final class NeighborWithComponentIDJoin
            implements JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> join(
                Tuple2<Long, Long> vertexWithComponent, Tuple2<Long, Long> edge) {
            return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
        }
    }

    /**
     * Emit the candidate (Vertex-ID, Component-ID) pair if and only if the candidate component ID
     * is less than the vertex's current component ID.
     */
    @ForwardedFieldsFirst("*")
    public static final class ComponentIdFilter
            implements FlatJoinFunction<
                    Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        @Override
        public void join(
                Tuple2<Long, Long> candidate,
                Tuple2<Long, Long> old,
                Collector<Tuple2<Long, Long>> out) {
            if (candidate.f1 < old.f1) {
                out.collect(candidate);
            }
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static DataSet<Long> getVertexDataSet(ExecutionEnvironment env, ParameterTool params) {
        if (params.has("vertices")) {
            return env.readCsvFile(params.get("vertices"))
                    .types(Long.class)
                    .map(
                            new MapFunction<Tuple1<Long>, Long>() {
                                public Long map(Tuple1<Long> value) {
                                    return value.f0;
                                }
                            });
        } else {
            System.out.println(
                    "Executing Connected Components example with default vertices data set.");
            System.out.println("Use --vertices to specify file input.");
            return ConnectedComponentsData.getDefaultVertexDataSet(env);
        }
    }

    private static DataSet<Tuple2<Long, Long>> getEdgeDataSet(
            ExecutionEnvironment env, ParameterTool params) {
        if (params.has("edges")) {
            return env.readCsvFile(params.get("edges"))
                    .fieldDelimiter(" ")
                    .types(Long.class, Long.class);
        } else {
            System.out.println(
                    "Executing Connected Components example with default edges data set.");
            System.out.println("Use --edges to specify file input.");
            return ConnectedComponentsData.getDefaultEdgeDataSet(env);
        }
    }
}
