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


import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.ops.FragmentContextImpl;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LogRecordReader.class);
  private static final int MAX_RECORDS_PER_BATCH = 8096;

  private String inputPath;
  private BufferedReader reader;
  private DrillBuf buffer;
  private VectorContainerWriter writer;
  private LogFormatPlugin.LogFormatConfig config;
  private int lineCount;
  private Pattern r;

  private List<String> fieldNames;
  private List<String> dataTypes;
  private boolean errorOnMismatch;
  private String dateFormat;
  private String timeFormat;
  private java.text.DateFormat df;
  private java.text.DateFormat tf;
  private long time;

  public LogRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem,
                         List<SchemaPath> columns, LogFormatPlugin.LogFormatConfig config) throws OutOfMemoryException {
    try {
      Path hdfsPath = new Path(inputPath);
      Configuration conf = new Configuration();
      FSDataInputStream fsStream = fileSystem.open(hdfsPath);
      CompressionCodecFactory factory = new CompressionCodecFactory(conf);
      CompressionCodec codec = factory.getCodec(hdfsPath);
      if (codec == null) {
        reader = new BufferedReader(new InputStreamReader(fsStream.getWrappedStream(), "UTF-8"));
      } else {
        CompressionInputStream comInputStream = codec.createInputStream(fsStream.getWrappedStream());
        reader = new BufferedReader(new InputStreamReader(comInputStream));
      }
      this.inputPath = inputPath;
      this.lineCount = 0;
      this.config = config;
      this.buffer = fragmentContext.getManagedBuffer(4096);
      setColumns(columns);

    } catch (IOException e) {
      logger.debug("Log Reader Plugin: " + e.getMessage());
    }
  }

  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output);
    String regex = config.getPattern();
    r = Pattern.compile(regex);

    fieldNames = config.getFieldNames();
    dataTypes = config.getDataTypes();
    dateFormat = config.getDateFormat();
    timeFormat = config.getTimeFormat();
    errorOnMismatch = config.getErrorOnMismatch();

    /*
    This section will check for;
    1.  Empty regex
    2.  Invalid Regex
    3.  Empty date string if the date format is used
    4.  No capturing groups in the regex
    5.  Incorrect number of data types
    6.  Invalid data types
     */
    //(\\d{6})\\s(\\d{2}:\\d{2}:\\d{2})\\s+(\\d+)\\s(\\w+)\\s+(.+)
    if( regex.isEmpty() ){
      throw UserException.parseError().message("Log parser requires a valid, non-empty regex in the plugin configuration").build(logger);
    }

    //Set up date formats
    if( dataTypes.contains("DATE") || dataTypes.contains("TIMESTAMP")) {
      if (dateFormat != null && !dateFormat.isEmpty()) {
        df = new java.text.SimpleDateFormat(dateFormat);
      } else {
        throw new java.time.DateTimeException("Invalid date format.  The date formatting string was empty.  Please specify a valid date format string in the configuration for this data source.");
      }
    }

    if( dataTypes.contains("TIME")) {
      if (timeFormat != null && !timeFormat.isEmpty()) {
        tf = new java.text.SimpleDateFormat(timeFormat);
      } else {
        throw new java.time.DateTimeException("Invalid time format.  The time formatting string was empty.  Please specify a valid time format string in the configuration for this data source.");
      }
    }

  }

  public int next() {
    this.writer.allocate();
    this.writer.reset();

    int recordCount = 0;

    try {
      BaseWriter.MapWriter map = this.writer.rootAsMap();
      String line = null;

      while (recordCount < MAX_RECORDS_PER_BATCH && (line = this.reader.readLine()) != null) {
        lineCount++;

        // Skip empty lines
        if (line.trim().length() == 0) {
          continue;
        }

        this.writer.setPosition(recordCount);
        map.start();

        Matcher m = r.matcher(line);

        //TODO Move this to setup
        if (m.groupCount() == 0) {
          throw new ParseException(
              "Invalid Regular Expression: No Capturing Groups", 0
          );
        } else if (m.groupCount() != (fieldNames.size())) {
          throw new ParseException(
              "Invalid Regular Expression: Field names do not match capturing groups.  There are " + m.groupCount() + " captured groups in the data and " + fieldNames.size() + " specified in the configuration.", 0);
        } else if ((dataTypes == null) || m.groupCount() != dataTypes.size()) {
          //If the number of data types is not correct, create a list of varchar
          dataTypes = new ArrayList<String>();
          for (int i = -0; i < m.groupCount(); i++) {
            dataTypes.add("VARCHAR");
          }
        }

        if (m.find()) {
          for (int i = 1; i <= m.groupCount(); i++) {

            String fieldName = fieldNames.get(i - 1);
            String type = dataTypes.get(i - 1);
            String fieldValue;

            fieldValue = m.group(i);

            if (fieldValue == null) {
              fieldValue = "";
            }

            if (type.toUpperCase().equals("INT") || type.toUpperCase().equals("INTEGER")) {
              map.integer(fieldName).writeInt(Integer.parseInt(fieldValue));
            } else if (type.toUpperCase().equals("DOUBLE") || type.toUpperCase().equals("FLOAT8")) {
              map.float8(fieldName).writeFloat8(Double.parseDouble(fieldValue));
            } else if (type.toUpperCase().equals("FLOAT") || type.toUpperCase().equals("FLOAT4")) {
              map.float4(fieldName).writeFloat4(Float.parseFloat(fieldValue));
            } else if (type.toUpperCase().equals("DATE")) {
              try {
                java.util.Date d = df.parse(fieldValue);
                long milliseconds = d.getTime();
                map.date(fieldName).writeDate(milliseconds);
              } catch (ParseException e) {
                if (errorOnMismatch) {
                  throw new ParseException(
                      "Date Format String " + dateFormat + " does not match date string " + fieldValue + " on line " + lineCount + ".", 0
                  );
                }
              }
            } else if (type.toUpperCase().equals("TIMESTAMP")) {
              try {
                java.util.Date d = df.parse(fieldValue);
                long milliseconds = d.getTime();
                map.timeStamp(fieldName).writeTimeStamp(milliseconds);
              } catch (ParseException e) {
                if (errorOnMismatch) {
                  throw new ParseException(
                      "Date Format String " + dateFormat + " does not match date string " + fieldValue + " on line " + lineCount + ".", 0
                  );
                }
              }
            } else if (type.toUpperCase().equals("TIME")) {
              java.util.Date t = tf.parse(fieldValue);

              int milliseconds = (int)((t.getHours() * 3600000) +
                  (t.getMinutes() *  60000) +
                  (t.getSeconds() * 1000));

              map.time(fieldName).writeTime(milliseconds);
            } else {
              byte[] bytes = fieldValue.getBytes("UTF-8");

              int stringLength = bytes.length;
              this.buffer.setBytes(0, bytes, 0, stringLength);
              map.varChar(fieldName).writeVarChar(0, stringLength, buffer);
            }
          }
        } else {
          if (errorOnMismatch) {
            throw new ParseException("Line does not match pattern: " + inputPath + "\n" + lineCount + ":\n" + line, 0);
          } else {
            String fieldName = "unmatched_lines";
            byte[] bytes = line.getBytes("UTF-8");
            this.buffer.setBytes(0, bytes, 0, bytes.length);
            map.varChar(fieldName).writeVarChar(0, bytes.length, buffer);
          }
        }

        map.end();
        recordCount++;
      }

      this.writer.setValueCount(recordCount);
      return recordCount;

    } catch (final Exception e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  public void close() throws Exception {
    this.reader.close();
  }
}
