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
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;

public class ImageRecLocal {

    public static final String TIKA_CONFIG_XML = "tika-config-classpath-model.xml";

    public static void main(String[] args) throws Exception {

    //create parser as per desired parser
    TikaConfig config;
    try (InputStream stream = ImageRecLocal.class.getClassLoader()
            .getResourceAsStream(TIKA_CONFIG_XML)){
        config = new TikaConfig(stream);
    }
    Tika parser = new Tika(config);
    //sample file
    File imageFile = new File("data/gun.jpg");
    Metadata meta = new Metadata();
    parser.parse(imageFile, meta);
    //retrieve objects from the metadata
    System.out.println(Arrays.toString(meta.getValues("OBJECT")));
    }
}
