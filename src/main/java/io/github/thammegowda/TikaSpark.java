package io.github.thammegowda;
/*
 * Copyright 2017 Thamme Gowda
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TikaSpark {

    private static final Logger LOG = LoggerFactory.getLogger(TikaSpark.class);

    @Option(name = "--in", aliases = "-in", required = true, usage = "Path to input sequence file. " +
            "This file should be in the same format as the output of Local2SeqFile.")
    private String input;

    @Option(name = "--out", aliases = "-out", required = true, usage = "Path to output file.")
    private String output;

    @Option(name = "--spark-master", aliases = "-sm", usage = "Spark master URL.")
    private String sparkMaster = "local[*]";

    static class TikaHolder {
        static final String CONF_FILE = "tika-config-classpath-model.xml";
        static final Tika tika;
        static {
            try {
                try(InputStream stream = TikaHolder.class.getClassLoader()
                        .getResourceAsStream(CONF_FILE)) {
                    tika = new Tika(new TikaConfig(stream));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void run() throws IOException {
        FileSystem fs = DistributedFileSystem.get(new Configuration());
        Path inpath = new Path(input);
        Path outpath = new Path(output);
        if (!fs.exists(inpath)) {
            throw new IllegalArgumentException("Input file not found: " + inpath);
        }
        if (fs.exists(outpath)) {
            throw new IllegalArgumentException("Output file exists, Not overwriting it: " + inpath);
        }

        SparkConf conf = new SparkConf();
        conf.setMaster(sparkMaster);
        conf.setAppName(getClass().getSimpleName() + "::" + System.currentTimeMillis());
        JavaSparkContext ctx = new JavaSparkContext(conf);

        //STEP1: READ
        JavaPairRDD<Text, BytesWritable> rdd = ctx.sequenceFile(input, Text.class, BytesWritable.class);
                //.mapToPair(rec -> new Tuple2<>(new Text(rec._1()), new BytesWritable(rec._2().getBytes())));
        //STEP2: PARSE
        JavaPairRDD<Text, Metadata> parsedRDD = rdd.mapToPair(
                (PairFunction<Tuple2<Text, BytesWritable>, Text, Metadata>) rec -> {
                    Metadata md = new Metadata();
                    try (ByteArrayInputStream stream = new ByteArrayInputStream(rec._2().getBytes())) {
                        String content = TikaHolder.tika.parseToString(stream, md);
                        md.add("CONTENT", content);
                    }
                    return new Tuple2<>(rec._1(), md);
                });
        //STEP3: FORMAT
        JavaRDD<String> outRDD = parsedRDD.map((Function<Tuple2<Text, Metadata>, String>) rec -> {
            String key = rec._1().toString();
            Metadata metadata = rec._2();
            JSONObject object = new JSONObject();
            for (String name : metadata.names()) {
                if (metadata.isMultiValued(name)) {
                    JSONArray arr = new JSONArray();
                    for (String val : metadata.getValues(name)) {
                        arr.add(val);
                    }
                    object.put(name, arr);
                } else {
                    object.put(name, metadata.get(name));
                }
            }
            return key + "\t\t" + object.toJSONString();
        });
        //STEP4: SAVE
        LOG.info("Saving at " + outpath);
        outRDD.saveAsTextFile(output);
        LOG.info("Stopping");
        ctx.stop();
    }

    public static void main(String[] args) throws IOException {
        TikaSpark tikaSpark = new TikaSpark();
        CmdLineParser parser = new CmdLineParser(tikaSpark);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
            return;
        }
        tikaSpark.run();
    }
}
