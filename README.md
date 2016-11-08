
# Drill Logfile Plugin
Plugin for Apache Drill that allows Drill to read and query log files of any format. 

If you wanted to analyze log files such as the MySQL log sample shown below using Drill, it may be possible using various string fucntions, or you could write a UDF specific to this data type as shown here: http://www.dremio.com/blog/querying-google-analytics-json-with-a-custom-sql-function/.  However, this is time consuming, difficult and results in some unnecessarily complex queries.

```
070823 21:00:32       1 Connect     root@localhost on test1
070823 21:00:48       1 Query       show tables
070823 21:00:56       1 Query       select * from category
070917 16:29:01      21 Query       select * from location
070917 16:29:12      21 Query       select * from location where id = 1 LIMIT 1
```
This plugin will allow you to configure Drill to directly query logfiles of any configuration.

##Installation:
This library has no dependencies, so simply build this using Maven by typing:
`mvn clean install -DskipTests` 
Next, go to the `targets/` directory and copy the `.jar` file to `<path to drill>/jars/3rdParty/`.  Alternatively you can directly download the `.jar` file from the releases page here:

##Usage
After installing the `.jar` file, go to the server configuration and add the following section in the `<extensions>` section of `dfs`.  In order to use the plugin, you will have to configure the `dfs` plugin on in your Storage Plugins section. 

### Configuration Options
* **`pattern`**:  This is the regular expression which defines how the log file lines will be split.  You must enclose the parts of the regex in grouping parentheses that you wish to extract.  Note that this plugin uses Java regular expressions and requires that shortcuts such as `\d` have an additional slash:  ie `\\d`.
* **`fieldNames`**:  This is a list of field names which you are extracting. Note that you must have the same number of fields as extracting groups in your pattern.
* **`type`**:  This tells Drill which extension to use.  In this case, it must be `log`.
* **`extensions`**:  This option tells Drill which file extensions should be mapped to this configuration.  Note that you can have multiple configurations of this plugin to allow you to query various log files.

### Example Usage:
The configuration below demonstrates how to configure Drill to query the example MySQL log file shown above.
```
log": {
      "type": "log",
      "extensions": [
        "log"
      ],
      "fieldNames": [
        "date",
        "time",
        "pid",
        "action",
        "query"
      ],
      "pattern": "(\\d{6})\\s(\\d{2}:\\d{2}:\\d{2})\\s+(\\d+)\\s(\\w+)\\s+(.+)"
    }
  }
 ```

