/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.trino;

import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/** Trino {@link ConnectorFactory}. */
public abstract class TrinoConnectorFactoryBase implements ConnectorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TrinoConnectorFactoryBase.class);

    // see https://trino.io/docs/current/connector/hive.html#hive-general-configuration-properties
    private static final String HADOOP_CONF_FILES_KEY = "hive.config.resources";
    // see org.apache.paimon.utils.HadoopUtils
    private static final String HADOOP_CONF_PREFIX = "hadoop.";

    @Override
    public String getName() {
        return "paimon";
    }

    @Override
    public Connector create(
            String catalogName, Map<String, String> config, ConnectorContext context) {
        config = new HashMap<>(config);
        if (config.containsKey(HADOOP_CONF_FILES_KEY)) {
            for (String hadoopXml : config.get(HADOOP_CONF_FILES_KEY).split(",")) {
                try {
                    readHadoopXml(hadoopXml, config);
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to read hadoop xml file " + hadoopXml + ", skipping this file.",
                            e);
                }
            }
        }

        return new TrinoConnector(
                new TrinoMetadata(Options.fromMap(config)),
                new TrinoSplitManager(),
                new TrinoPageSourceProvider());
    }

    private void readHadoopXml(String path, Map<String, String> config) throws Exception {
        path = path.trim();
        if (path.isEmpty()) {
            return;
        }

        File xmlFile = new File(path);
        NodeList propertyNodes =
                DocumentBuilderFactory.newInstance()
                        .newDocumentBuilder()
                        .parse(xmlFile)
                        .getElementsByTagName("property");
        for (int i = 0; i < propertyNodes.getLength(); i++) {
            Node propertyNode = propertyNodes.item(i);
            if (propertyNode.getNodeType() == 1) {
                Element propertyElement = (Element) propertyNode;
                String key = propertyElement.getElementsByTagName("name").item(0).getTextContent();
                String value =
                        propertyElement.getElementsByTagName("value").item(0).getTextContent();
                if (!StringUtils.isNullOrWhitespaceOnly(value)) {
                    config.putIfAbsent(HADOOP_CONF_PREFIX + key, value);
                }
            }
        }
    }
}
