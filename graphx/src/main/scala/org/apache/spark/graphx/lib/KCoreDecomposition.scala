/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.graphx.lib


import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.graphx._

/** K-core decomposition algorithm to compute the k-coreness of vertex for undirected graph.
  *
  * Note that the undirected edge such as a-b of the graph is represented as two directed edges,
  * that is, a->b and b->a。
  */

object KCoreDecomposition extends Logging{
    /**
     * K-core decomposition algorithm implementation.Compute the k-coreness of
     * each vertex and return a graph with the vertex value containing the k-coreness
     *
     * @tparam VD the vertex attribute type
     * @tparam ED the edge attribute type
     *
     * @param graph the graph for which to compute the k-coreness
     *
     * @return a graph with vertex attributes containing the k-coreness
     */
    def run[VD:ClassTag,ED:ClassTag](graph: Graph[VD,ED]):Graph[Int,ED]= {
        val ksGraph: Graph[(Int,Int),Int] = graph
            .outerJoinVertices(graph.outDegrees){(vid,vdata,deg)=>deg.getOrElse(0)}
            //Set the vertex attributes to the inital dregee and the status of vertex
            // is flagged as 0, which means k-coreness of the vertex doesn't change by now.
            .mapVertices((id,attr)=>(attr,0))

        def vertexProgram(id: VertexId,attr: (Int,Int),msgSum: Vector[Int]):(Int,Int) = {
            if(msgSum.head == -1)
                (attr._1,2)
            else {
                var flag = 0
                var newKCore = attr._1
                for(i <- (1 to attr._1).reverse if msgSum.count(_ >= i) >= i && flag == 0){
                    newKCore = i
                    flag = 1
                }
                if(newKCore != attr._1)
                    (newKCore,1)
                else
                    (attr._1,0)
            }
        }

        def sendMessage(edge: EdgeTriplet[(Int,Int),ED])={
            if(edge.srcAttr._2 == 2)
                Iterator((edge.srcId,Vector(edge.dstAttr._1)))
            else if(edge.srcAttr._2 == 1 && edge.srcAttr._1 < edge.dstAttr._1)
                Iterator((edge.dstId,Vector(-1)))
            else
                Iterator.empty
        }

        def messageCombiner(a: Vector[Int],b: Vector[Int]): Vector[Int] = {
            if(a.head == -1)
                a
            else
                a ++ b
        }

        val initalMessage = -1

        val g = Pregel(ksGraph,initalMessage,activeDirection = EdgeDirection.Out)(
                vertexProgram,sendMessage,messageCombiner)
        g.mapVertices((vid,attr)=>attr._1)

    }
}