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

package org.apache.hadoop.mapreduce.lib.map;

import java.io.IOException;
import java.net.URI;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import org.apache.hadoop.mapred.JobStatus;


/**
 * A {@link Mapper} which wraps a given one to allow custom 
 * {@link Mapper.Context} implementations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
    extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  
  /**
   * Get a wrapped {@link Mapper.Context} for custom implementations.
   * @param mapContext <code>MapContext</code> to be wrapped
   * @return a wrapped <code>Mapper.Context</code> for custom implementations
   */
  public Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context
  getMapContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
    return new Context(mapContext);
  }

  public Context getContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
    return new Context(mapContext);
  }

  public Context getContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext, boolean shouldSplit) {
    return new Context(mapContext, shouldSplit);
  }

  public Context getContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext,
                            boolean shouldSplit, Map<String, Integer> globalHistogram) {
    return new Context(mapContext, shouldSplit, globalHistogram);
  }

  public Context getContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext,
                            boolean shouldSplit, Map<String, Integer> globalHistogram,
                            Map<String, String> globalLookupTable) {
    return new Context(mapContext, shouldSplit, globalHistogram, globalLookupTable);
  }

  @InterfaceStability.Evolving
  public class Context 
      extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context {

    protected MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext;

    // calcuate local key distribution
    protected Map<KEYOUT, Integer> localHistogram;
    protected Map<String, Integer> globalHistogram;
    protected Map<String, String> globalLookupTable;
    protected boolean shouldSplit;

    public Context(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext) {
      this.mapContext = mapContext;
      this.localHistogram = new HashMap<>();
    }

    public Context(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext, boolean shouldSplit) {
      this.mapContext = mapContext;
      this.localHistogram = new HashMap<>();
      this.shouldSplit = shouldSplit;
    }

    public Context(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext,
                   boolean shouldSplit, Map<String, Integer> globalHistogram) {
      this.mapContext = mapContext;
      this.localHistogram = new HashMap<>();
      this.shouldSplit = shouldSplit;
      this.globalHistogram = globalHistogram;
    }

    public Context(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext,
                   boolean shouldSplit, Map<String, Integer> globalHistogram,
                   Map<String, String> globalLookupTable) {
      this.mapContext = mapContext;
      this.localHistogram = new HashMap<>();
      this.shouldSplit = shouldSplit;
      this.globalHistogram = globalHistogram;
      this.globalLookupTable = globalLookupTable;
    }

    /**
     * Get the input split for this map.
     */
    public InputSplit getInputSplit() {
      return mapContext.getInputSplit();
    }

    public Map<KEYOUT, Integer> getLocalHistogram() {
      return this.localHistogram;
    }
    public Map<String, Integer> getLocalStringHistogram() {
      Map<String, Integer> map = new HashMap<>();
      for (KEYOUT key: localHistogram.keySet()) {
        map.put(key.toString(), localHistogram.get(key));
      }
      return map;
    }

    public Map<String, Integer> getGlobalHistogram() { return this.globalHistogram; }


    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return mapContext.getCurrentKey();
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return mapContext.getCurrentValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return mapContext.nextKeyValue();
    }

    @Override
    public Counter getCounter(Enum<?> counterName) {
      return mapContext.getCounter(counterName);
    }

    @Override
    public Counter getCounter(String groupName, String counterName) {
      return mapContext.getCounter(groupName, counterName);
    }

    @Override
    public OutputCommitter getOutputCommitter() {
      return mapContext.getOutputCommitter();
    }

    private int median(Map<String, Integer> histogram) {
      if (histogram.size() == 0) {
        return 1;
      }

      int[] list = new int[histogram.size()];
      int i = 0;
      for (int value: histogram.values()) {
        list[i++] = value;
      }
      Arrays.sort(list);
      return list[(int) list.length / 2];
    }

    @Override
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {

//      System.out.println("WrappedMapper Write (" + key + ", " + value + ")");

      if (!this.shouldSplit) {
//        System.out.println("SPLIT!");
        KEYOUT keyOut = (KEYOUT) new Text(key.toString());
        if (localHistogram.containsKey(keyOut)) {
          localHistogram.put(keyOut, localHistogram.get(keyOut) + 1);
        } else {
          localHistogram.put(keyOut, 1);
        }

        if (globalHistogram.containsKey(key.toString())) {
          globalHistogram.put(key.toString(), globalHistogram.get(key.toString()) + 1);
        } else {
          globalHistogram.put(key.toString(), 1);
        }

        mapContext.write(key, value);

      }
      else {

        // compute median from global histogram
//        int median = median(globalHistogram) * 5;
        int median = median(globalHistogram);
        int count = globalHistogram.get(key.toString());

//        System.out.println(Arrays.asList(globalHistogram));
//        System.out.println("Median: " + Integer.toString(median) + " Count: " + Integer.toString(count));

        // don't split
        if (count < median) {
          mapContext.write(key, value);
        }
        // split key
        else {
          int randomNum = ThreadLocalRandom.current().nextInt(0, (int) count / median + 1);
          String temp = (key.toString() + "/" + randomNum);
//          System.out.println("Splited Key, Value = (" + temp + ", " + value.toString() + ")");
          KEYOUT newKey = (KEYOUT) new Text(temp);
          mapContext.write(newKey, value);
          globalLookupTable.put(temp, key.toString());
        }
      }

    }

    @Override
    public String getStatus() {
      return mapContext.getStatus();
    }

    @Override
    public TaskAttemptID getTaskAttemptID() {
      return mapContext.getTaskAttemptID();
    }

    @Override
    public void setStatus(String msg) {
      mapContext.setStatus(msg);
    }

    @Override
    public Path[] getArchiveClassPaths() {
      return mapContext.getArchiveClassPaths();
    }

    @Override
    public String[] getArchiveTimestamps() {
      return mapContext.getArchiveTimestamps();
    }

    @Override
    public URI[] getCacheArchives() throws IOException {
      return mapContext.getCacheArchives();
    }

    @Override
    public URI[] getCacheFiles() throws IOException {
      return mapContext.getCacheFiles();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
        throws ClassNotFoundException {
      return mapContext.getCombinerClass();
    }

    @Override
    public Configuration getConfiguration() {
      return mapContext.getConfiguration();
    }

    @Override
    public Path[] getFileClassPaths() {
      return mapContext.getFileClassPaths();
    }

    @Override
    public String[] getFileTimestamps() {
      return mapContext.getFileTimestamps();
    }

    @Override
    public RawComparator<?> getCombinerKeyGroupingComparator() {
      return mapContext.getCombinerKeyGroupingComparator();
    }

    @Override
    public RawComparator<?> getGroupingComparator() {
      return mapContext.getGroupingComparator();
    }

    @Override
    public Class<? extends InputFormat<?, ?>> getInputFormatClass()
        throws ClassNotFoundException {
      return mapContext.getInputFormatClass();
    }

    @Override
    public String getJar() {
      return mapContext.getJar();
    }

    @Override
    public JobID getJobID() {
      return mapContext.getJobID();
    }

    @Override
    public String getJobName() {
      return mapContext.getJobName();
    }

    @Override
    public boolean getJobSetupCleanupNeeded() {
      return mapContext.getJobSetupCleanupNeeded();
    }

    @Override
    public boolean getTaskCleanupNeeded() {
      return mapContext.getTaskCleanupNeeded();
    }

    @Override
    public Path[] getLocalCacheArchives() throws IOException {
      return mapContext.getLocalCacheArchives();
    }

    @Override
    public Path[] getLocalCacheFiles() throws IOException {
      return mapContext.getLocalCacheFiles();
    }

    @Override
    public Class<?> getMapOutputKeyClass() {
      return mapContext.getMapOutputKeyClass();
    }

    @Override
    public Class<?> getMapOutputValueClass() {
      return mapContext.getMapOutputValueClass();
    }

    @Override
    public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
        throws ClassNotFoundException {
      return mapContext.getMapperClass();
    }

    @Override
    public int getMaxMapAttempts() {
      return mapContext.getMaxMapAttempts();
    }

    @Override
    public int getMaxReduceAttempts() {
      return mapContext.getMaxReduceAttempts();
    }

    @Override
    public int getNumReduceTasks() {
      return mapContext.getNumReduceTasks();
    }

    @Override
    public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
        throws ClassNotFoundException {
      return mapContext.getOutputFormatClass();
    }

    @Override
    public Class<?> getOutputKeyClass() {
      return mapContext.getOutputKeyClass();
    }

    @Override
    public Class<?> getOutputValueClass() {
      return mapContext.getOutputValueClass();
    }

    @Override
    public Class<? extends Partitioner<?, ?>> getPartitionerClass()
        throws ClassNotFoundException {
      return mapContext.getPartitionerClass();
    }

    @Override
    public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
        throws ClassNotFoundException {
      return mapContext.getReducerClass();
    }

    @Override
    public RawComparator<?> getSortComparator() {
      return mapContext.getSortComparator();
    }

    @Override
    public boolean getSymlink() {
      return mapContext.getSymlink();
    }

    @Override
    public Path getWorkingDirectory() throws IOException {
      return mapContext.getWorkingDirectory();
    }

    @Override
    public void progress() {
      mapContext.progress();
    }

    @Override
    public boolean getProfileEnabled() {
      return mapContext.getProfileEnabled();
    }

    @Override
    public String getProfileParams() {
      return mapContext.getProfileParams();
    }

    @Override
    public IntegerRanges getProfileTaskRange(boolean isMap) {
      return mapContext.getProfileTaskRange(isMap);
    }

    @Override
    public String getUser() {
      return mapContext.getUser();
    }

    @Override
    public Credentials getCredentials() {
      return mapContext.getCredentials();
    }
    
    @Override
    public float getProgress() {
      return mapContext.getProgress();
    }
  }
}
