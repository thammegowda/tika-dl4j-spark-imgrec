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
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.tika.io.IOUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.util.Iterator;

/**
 * This tool creates a large sequence file by combing many small files on local file system.
 * This tool offers command line interface.
 */
public class Local2SeqFile {

    @Option(name = "--in", aliases = "-in", required = true,
            usage = "Path to input on local file system. " +
                    "This could be path to a parent directory or a file having list of paths.")
    private String input;

    @Option(name = "--out", aliases = "-out", required = true,
            usage = "Path to output sequence file")
    private String output;

    @Option(name = "--min-size", aliases = "-min",
            usage = "Files having fewer number of bytes than this number will be skipped.")
    private long minFileSize = 1;

    @Option(name = "--max-size", aliases = "-max",
            usage = "Files having more bytes than this number will be skipped. " +
                    "Note: the value type of sequence file is hadoop.io.BytesWritable, " +
                    "meaning that the content will be held in memory. " +
                    "Thus, setting it to a large value could cause memory overflow")
    private long maxFileSize = 64 * 1024 * 1024;

    private FileSystem localFs;
    private FileSystem distribFs;

    public Local2SeqFile() throws IOException {
        super();
        Configuration config = new Configuration();
        localFs = LocalFileSystem.get(config);
        distribFs = DistributedFileSystem.get(config);
    }

    private static class FileListIterator implements RemoteIterator<FileStatus>,
            Iterator<FileStatus>, Closeable {

        private FileStatus next;
        private BufferedReader reader;
        private FileSystem fs;

        public FileListIterator(InputStream content, FileSystem fs) {
            reader = new BufferedReader(new InputStreamReader(content));
            next = getNext();
            this.fs = fs;
        }

        private FileStatus getNext() {
            if (reader == null) {
                throw new IllegalStateException("Reader already closed");
            }
            try {
                String line = reader.readLine();
                if (line == null) { // end of stream
                    close();
                    return null;
                }
                line = line.trim();
                if (line.isEmpty()) {
                    return getNext();
                }
                return fs.getFileStatus(new Path(line));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
            reader = null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public FileStatus next() {
            FileStatus tmp = next;
            next = getNext();
            return tmp;
        }
    }

    private RemoteIterator<? extends FileStatus> readInput() throws IOException {
        Path inPath = new Path(input);
        if (!localFs.exists(inPath)) {
            throw new IllegalArgumentException("File " + input + " doesnt exists");
        }
        RemoteIterator<? extends FileStatus> files;
        if (localFs.isFile(inPath)) {
            try (FSDataInputStream content = localFs.open(inPath)) {
                files = new FileListIterator(content, localFs);
            }
        } else if (localFs.isDirectory(inPath)) {
            files = localFs.listFiles(inPath, true);
        } else {
            throw new RuntimeException("Unknown input file type");
        }
        return files;
    }

    private boolean filter(FileStatus file) {
        return file.isFile()
                && file.getLen() >= minFileSize
                && file.getLen() <= maxFileSize;
    }

    private void writeOutput(RemoteIterator<? extends FileStatus> input) throws IOException {
        Path outPath = new Path(output);
        if (distribFs.exists(outPath)) {
            throw new IllegalArgumentException("Output file already exists, Not overwriting it:" + output);
        }

        Writer writer = SequenceFile.createWriter(distribFs.getConf(),
                Writer.file(outPath),
                Writer.keyClass(Text.class),
                Writer.valueClass(BytesWritable.class),
                Writer.compression(SequenceFile.CompressionType.RECORD));
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        long skipped = 0;
        long copied = 0;
        while (input.hasNext()) {
            FileStatus next = input.next();
            if (filter(next)) {
                key.set(next.getPath().toString());
                FSDataInputStream stream = localFs.open(next.getPath());
                //CAUTION : this could cause memory overflow
                byte[] bytes = IOUtils.toByteArray(stream);
                value.set(bytes, 0, bytes.length);
                writer.append(key, value);
                copied++;
            } else {
                skipped++;
            }
        }
        writer.close();
        System.out.println("Files copied ::" + copied);
        System.out.println("Files skipped ::" + skipped);
    }

    public static void main(String[] args) throws CmdLineException, IOException {
        Local2SeqFile copier = new Local2SeqFile();
        CmdLineParser parser = new CmdLineParser(copier);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printUsage(System.out);
            return;
        }
        copier.writeOutput(copier.readInput());
    }
}
