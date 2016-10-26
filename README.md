<<<<<<< HEAD
drill-ltsv-plugin
====
Apache Drill plugin for [LTSV (Labeled Tab-separated Values)](http://ltsv.org/) files.

What is LTSV?
----
LTSV is a format for the text file like this:

```
label:value<TAB>label:value<TAB>label:value ...
```

Quote the explanation from the web site:

> Labeled Tab-separated Values (LTSV) format is a variant of Tab-separated Values (TSV). Each record in a LTSV file is represented as a single line. Each field is separated by TAB and has a label and a value. The label and the value have been separated by ‘:’. With the LTSV format, you can parse each line by spliting with TAB (like original TSV format) easily, and extend any fields with unique labels in no particular order.
> LTSV is simple and flexible. Parsing LTSV is super easy, and we can add new fields feel free. It’s very suitable for various logs.

This plugin adds LTSV suuport to Apache Drill.

Installation
----

Download `drill-ltsv-plugin-VERSION.jar` from the [release page](https://github.com/bizreach/drill-ltsv-plugin/releases) and put it into `DRILL_HOME/jars/3rdparty`.

Add ltsv format setting to the storage configuration as below:

```javascript
  "formats": {
    "ltsv": {
      "type": "ltsv",
      "extensions": [
        "ltsv"
      ]
    },
    ...
  }
```

Then you can query `*.ltsv` files on Apache Drill.
=======
# drill-logfile-plugin
Plugin for Apache Drill that parses generic log files
>>>>>>> 9ac6659327f6a6565a80a0346301785165415ffb
