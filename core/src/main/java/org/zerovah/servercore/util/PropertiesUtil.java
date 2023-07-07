package org.zerovah.servercore.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {

    private static final Logger LOGGER = LogManager.getLogger(PropertiesUtil.class);

    public static Properties getProperties(InputStream inStream) {
        Properties properties = new Properties();
        try {
            properties.load(inStream);
        } catch (Exception e) {
            LOGGER.error("", e);
        } finally {
            close(inStream);
        }
        return properties;
    }

    public static Properties getProperties(String pathFileName) {
        InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(pathFileName);
        try {
            Properties properties = new Properties();
            properties.load(inStream);
            return properties;
        } catch (Exception e) {
            LOGGER.error("", e);
            return null;
        } finally {
            close(inStream);
        }
    }

    private static void close(Closeable closeableObj) {
        try {
            if (closeableObj == null) {
                return;
            }
            closeableObj.close();
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    public static void loadPropertiesToSystem(String pathFileName) {
        InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(pathFileName);
        try {
            System.getProperties().load(inStream);
        } catch (Exception e) {
            LOGGER.error("不存在节点配置{}, 尝试读取核心配置文件", pathFileName);
            loadDefaultSystemConfig();
        } finally {
            close(inStream);
        }
    }

    private static void loadDefaultSystemConfig() {
        String coreConfig = System.getProperty("core.file");
        if (StringUtils.isBlank(coreConfig)) {
            return;
        }
        if (!Files.exists(Paths.get(coreConfig))) {
            LOGGER.warn("不存在核心配置文件:{}", coreConfig); return;
        }
        Map<String, Object> coreParams = parseConfigAnySyntax(coreConfig);
        System.getProperties().putAll(coreParams);
    }

    public static Map<String, Object> parseConfigAnySyntax(String configName) {
        if (configName.endsWith(".conf") || configName.endsWith(".json")
                || configName.endsWith(".properties")) {
            Config config = ConfigFactory.parseResourcesAnySyntax(configName);
            Map<String, Object> configs = new HashMap<>();
            for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
                configs.put(entry.getKey(), entry.getValue().unwrapped());
            }
            return configs;
        } else if (configName.endsWith(".yml") || configName.endsWith(".yaml")) {
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(configName);
            Map<String, Object> configs = Collections.emptyMap();
            try {
                Yaml yaml = new Yaml();
                configs = yaml.loadAs(in, HashMap.class);
            } catch (Exception e) {
                LOGGER.error("解析出错, {}", configName);
            } finally {
                close(in);
            }
            return configs;
        } else {
            LOGGER.warn("不合法的文件类型:" + configName);
            return Collections.emptyMap();
        }
    }

}
