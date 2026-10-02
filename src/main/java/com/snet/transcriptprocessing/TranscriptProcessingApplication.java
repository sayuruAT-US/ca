package com.snet.transcriptprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

/**
 * Main entry point for the Transcript Processing Service (ciap-kafka).
 *
 * External configuration is loaded from ONE directory — the same mechanism as
 * ciap-api (overrides + secrets on top of the bundled application.properties):
 *
 *   ${config.dir | CONFIG_DIR | /d01/ciap/config}/   (+ classpath fallback)
 *
 * Set the path with JVM arg -Dconfig.dir=/custom/path or env CONFIG_DIR=/custom/path.
 *
 * NOTE: give each service its OWN directory. ciap-api and ciap-kafka both ship an
 * application.properties, so pointing both CONFIG_DIRs at the same folder would let
 * one service pick up the other's application.properties. Run each unit with a
 * distinct CONFIG_DIR (e.g. /d01/ciap/config for api, /d01/ciap/kafka/config for kafka).
 */
@SpringBootApplication
public class TranscriptProcessingApplication {

    private static final Logger log = LoggerFactory.getLogger(TranscriptProcessingApplication.class);

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(TranscriptProcessingApplication.class);

        String configDir = System.getProperty("config.dir",
                System.getenv().getOrDefault("CONFIG_DIR", "/d01/ciap/config"));
        // spring.config.location REPLACES Spring's default search set. Order matters:
        // later locations WIN, so classpath (the bundled application.properties) is
        // listed first and the external directory last, so the external dir overrides.
        String location = "optional:classpath:/,optional:file:" + configDir + "/";
        log.info("Config location (single dir): {}", location);

        app.setDefaultProperties(Map.of("spring.config.location", location));
        app.run(args);
    }
}
