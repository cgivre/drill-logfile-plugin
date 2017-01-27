package org.apache.drill.exec.store.log;

/**
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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
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

    public LogRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem,
                            List<SchemaPath> columns, LogFormatPlugin.LogFormatConfig config) throws OutOfMemoryException {
        try {
            FSDataInputStream fsStream = fileSystem.open(new Path(inputPath));
            this.inputPath = inputPath;
            this.lineCount = 0;
            this.reader = new BufferedReader(new InputStreamReader(fsStream.getWrappedStream(), "UTF-8"));
            this.config = config;
            this.buffer = fragmentContext.getManagedBuffer(4096);
            setColumns(columns);

        } catch(IOException e){
            logger.debug("Log Reader Plugin: " + e.getMessage());
        }
    }

    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        this.writer = new VectorContainerWriter(output);
        String regex = config.pattern;
        r = Pattern.compile(regex);
    }

    public int next() {
        this.writer.allocate();
        this.writer.reset();

        int recordCount = 0;

        try {
            BaseWriter.MapWriter map = this.writer.rootAsMap();
            String line = null;

            while(recordCount < MAX_RECORDS_PER_BATCH &&(line = this.reader.readLine()) != null){
                lineCount++;

                // Skip empty lines
                if(line.trim().length() == 0){
                    continue;
                }

                this.writer.setPosition(recordCount);
                map.start();

                List<String> fieldNames = config.fieldNames;

                boolean errorOnMismatch = config.errorOnMismatch;

                Matcher m = r.matcher(line);

                if( m.groupCount() == 0 ) {
                    throw new ParseException(
                        "Invalid Regular Expression: No Capturing Groups" , 0
                    );
                }
                else if( m.groupCount() != (fieldNames.size())) {
                    throw new ParseException(
                        "Invalid Regular Expression: Field names do not match capturing groups.  There are " + m.groupCount() + " captured groups in the data and " + fieldNames
                            .size() + " specified in the configuration.", 0
                    );
                }

                if (m.find()) {
                    for( int i = 1; i <= m.groupCount(); i++ )
                    {
                        //TODO Add option for date fields
                        String fieldName  = fieldNames.get(i - 1);
                        String fieldValue;

                        fieldValue = m.group(i);

                        if( fieldValue == null){
                            fieldValue = "";
                        }
                        byte[] bytes = fieldValue.getBytes("UTF-8");

                        int stringLength = bytes.length;
                        /*if( stringLength > 256 ){
                            stringLength = 256;
                        }*/


                        /*
                        java.text.DateFormat df = new java.text.SimpleDateFormat("yyMMdd HHmm");
                        java.util.Date date;
                        long file_creation_date = 0;
                        try {
                            date = df.parse(String.valueOf( creation_date + " " + creation_time));
                            file_creation_date = date.getTime();
                        } catch( Exception e) {

                        }
                        map.timeStamp("File_creation_date").writeTimeStamp(file_creation_date);
                        */

                        /*this.buffer.reallocIfNeeded(stringLength);
                        this.buffer.setBytes(0, bytes, 0, stringLength);
                        map.varChar(fieldName).writeVarChar(0, stringLength, buffer);*/


                        this.buffer.setBytes(0, bytes, 0, stringLength);
                        map.varChar(fieldName).writeVarChar(0, stringLength, buffer);
                    }
                } else {
                    if( errorOnMismatch ) {
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

    private long reformatDate( String d ){
        long result = 0;
        java.text.DateFormat df = new java.text.SimpleDateFormat("yyMMdd");
        java.util.Date date;
        try {
            date = df.parse(String.valueOf(d));
            result = date.getTime();
        } catch( Exception e) {

        }

        return result;
    }
}
