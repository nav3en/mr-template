/**
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
package mrtemplate;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestMyMapReduce {
  private MapDriver<LongWritable, Text, Text, Text> mapDriver;
  private ReduceDriver<Text, Text, Text, Text> reduceDriver;
  
  @Before
  public void setup() {
    MyMapper mapper = new MyMapper();
    MyReducer reducer = new MyReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }
  
  @Test
  public void testMapper() throws IOException {
    String line = "theline";
    mapDriver.withInput(new LongWritable(0), new Text(line));

    Text expectedKey = new Text("outkey");
    Text expectedVal = new Text(line);
    mapDriver.withOutput(expectedKey, expectedVal);
    mapDriver.runTest();
  }
  
  @Test
  public void testReducer() {
    Text inKey = new Text("outkey");
    Text inVal = new Text("outval");

    reduceDriver.withInput(inKey, Arrays.asList(inVal));
    
    Text expectedKey = new Text("outkey");
    Text expectedVal = new Text("outval");

    reduceDriver.withOutput(expectedKey, expectedVal);
    reduceDriver.runTest();
  }
}
