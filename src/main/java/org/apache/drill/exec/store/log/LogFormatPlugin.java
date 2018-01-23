package org.apache.drill.exec.store.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

public class LogFormatPlugin extends EasyFormatPlugin<LogFormatPlugin.LogFormatConfig> {

  private static final boolean IS_COMPRESSIBLE = true;
  private static final String DEFAULT_NAME = "log";
  private LogFormatConfig config;

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogFormatPlugin.class);

  public LogFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new LogFormatConfig());
  }

  public LogFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config, LogFormatConfig formatPluginConfig) {
    super(name, context, fsConf, config, formatPluginConfig, true, false, false, IS_COMPRESSIBLE, formatPluginConfig.getExtensions(), DEFAULT_NAME);
    this.config = formatPluginConfig;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
                                      List<SchemaPath> columns, String userName) throws ExecutionSetupException {
    return new LogRecordReader(context, fileWork.getPath(), dfs, columns, config);
  }


  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    return null;
  }

  @JsonTypeName("log")
  public static class LogFormatConfig implements FormatPluginConfig {
    public List<String> extensions;
    public List<String> fieldNames;
    public List<String> dataTypes;
    public String pattern;
    public Boolean errorOnMismatch = false;
    public String dateFormat = null;
    public String timeFormat = null;

    private static final List<String> DEFAULT_EXTS = ImmutableList.of("log");

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      if (extensions == null) {
        return DEFAULT_EXTS;
      }
      return extensions;
    }

    @Override
    public int hashCode() {
      int result = pattern != null ? pattern.hashCode() : 0;
      result = 31 * result + (dateFormat != null ? dateFormat.hashCode() : 0) + (timeFormat != null ? timeFormat.hashCode() : 0);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (getClass() == obj.getClass()) {
        return true;
      }
      return false;
    }
  }

}
