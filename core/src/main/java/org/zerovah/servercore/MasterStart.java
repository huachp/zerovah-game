package org.zerovah.servercore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zerovah.servercore.cluster.base.Clusters;

public class MasterStart {
    
    private static final Logger LOGGER = LogManager.getLogger(MasterStart.class);
    
    public static void main(String[] args) {
        try {
            Clusters.startMaster();
        } catch (Exception e) {
            LOGGER.error("Master服务启动异常", e);
        }
    }

}
