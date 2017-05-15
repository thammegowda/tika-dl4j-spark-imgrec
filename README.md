# tika-dl4j-spark-imgrec
Image recognition on Spark cluster powered by Deeplearning4j and Apache Tika


## Build:

Before you build this project, download the model file and place it under `src/main/resources`

```
cd src/main/resources
wget -O inception-model-weights.h5 https://raw.githubusercontent.com/USCDataScience/dl4j-kerasimport-examples/98ec48b56a5b8fb7d54a2994ce9cb23bfefac821/dl4j-import-example/data/inception-model-weights.h5

$ md5sum inception-model-weights.h5
e0fff1809e92effa7e74f365951149ab  inception-model-weights.h5
```

```
# Build for all platforms

mvn clean package

# Build for a specific platform (thus to remove unnecessary native libs)
#PLATFORM=macosx-x86_64
#PLATFORM=windows-x86_64
PLATFORM=linux-x86_64
mvn clean package -Djavacpp.platform=$PLATFORM
```

## Run Examples:

This is a two step process.
## Step 1: Convert input files into a sequence File.

The key type = hadoop.io.Text
The value type = hadoop.io.BytesWritable

This project has a tool called `io.github.thammegowda.Local2SeqFile`
 to create a large sequence file by reading local files.

```bash
java -cp target/tika-dl4j-spark-imgrec-1.0-SNAPSHOT-jar-with-dependencies.jar \
 io.github.thammegowda.Local2SeqFile
 --in (-in) VAL      : Path to input on local file system. This could be path
                       to a parent directory or a file having list of paths.
 --max-size (-max) N : Files having more bytes than this number will be
                       skipped. Note: the value type of sequence file is
                       hadoop.io.BytesWritable, meaning that the content will
                       be held in memory. Thus, setting it to a large value
                       could cause memory overflow (default: 67108864)
 --min-size (-min) N : Files having fewer number of bytes than this number will
                       be skipped. (default: 1)
 --out (-out) VAL    : Path to output sequence file

```

#### Example:

```bash
java -cp target/tika-dl4j-spark-imgrec-1.0-SNAPSHOT-jar-with-dependencies.jar \
  io.github.thammegowda.Local2SeqFile -in data/ -out data-out
```

## Step 2: Run Tika on spark

This step requires sequence file as input file.
The output file is stored as simple text file of key - value records.
The keys are file paths. Values are json objects of metadata.
If the input files are JPEGs, 'OBJECT' key will have the output of object recognition.

Note: Though this project was created to run image recognition on spark via tika-dl4j,
it is not limited to just images. It can be used to process all other file types on spark using tika,
 in those cases 'CONTENT' key may provide extracted text.
Caveat: It loads the entire file contents into memory, so it may not work out of the box for large files such as videos!

```bash
$ java -cp target/tika-dl4j-spark-imgrec-1.0-SNAPSHOT-jar-with-dependencies.jar \
io.github.thammegowda.TikaSpark
 --in (-in) VAL           : Path to input sequence file. This file should be in
                            the same format as the output of Local2SeqFile.
 --out (-out) VAL         : Path to output file.
 --spark-master (-sm) VAL : Spark master URL. (default: local[*])

```

#### Example

```bash
java -cp target/tika-dl4j-spark-imgrec-1.0-SNAPSHOT-jar-with-dependencies.jar \
 io.github.thammegowda.TikaSpark -in data-out -out data-out3
```

## Developers

Thamme Gowda

## License

Apache License 2.0

## Questions ?

[https://twitter.com/intent/tweet?text=%40thammegowda%20Hey%20TG%2C%20I%20have%20%20a%20question&source=webclient](Send them to me)

